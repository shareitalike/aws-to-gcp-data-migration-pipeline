"""
RetailEdge Global — Cloud Run File Validation Service
======================================================
Cloud Data Architecture Group | GCP Data Engineering Engagement

Purpose:
    Event-driven validation gate triggered by GCS object creation events.
    For every file landing in the GCS Raw bucket, this service:
      1. Computes an MD5 hash and checks Firestore for duplicates
      2. Validates the Parquet schema against the registered data contract
      3. Routes valid files to GCS Validated or GCS Quarantine
      4. Sends Slack alerts on validation failure

Architecture Role:
    This is Layer 1 of the three-layer schema drift defense.
    It is also the first idempotency checkpoint in the pipeline.

Design Decisions:
    - Cloud Run (not Airflow task) because validation runs asynchronously on
      file arrival — does not block or wait for Airflow scheduling
    - Firestore (not SQLite or in-memory dict) because Cloud Run is stateless
      and ephemeral — multiple instances must share dedup state
    - PyArrow schema read (not full file read) — reads only the Parquet footer
      metadata, not the data rows — extremely fast and memory-efficient
    - EXPECTED_SCHEMAS only checks REQUIRED columns — extra upstream columns
      are intentionally allowed through (Layer 2 Spark .select() handles them)
"""

import os
import hashlib
import logging
from typing import Optional

import functions_framework
import pyarrow.parquet as pq
from google.cloud import storage, firestore
import requests

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration (injected via Cloud Run environment variables) ────────────────
PROJECT_ID          = os.environ["GCP_PROJECT_ID"]
VALIDATED_BUCKET    = os.environ["VALIDATED_BUCKET"]      # retailedge-validated-prod
QUARANTINE_BUCKET   = os.environ["QUARANTINE_BUCKET"]     # retailedge-quarantine-prod
SLACK_WEBHOOK_URL   = os.environ.get("SLACK_WEBHOOK_URL", "")
FIRESTORE_COLLECTION = "file_ingestion_registry"

# ── Data Contract — Machine-Readable Schema Registry ──────────────────────────
#
# DESIGN DECISION: Only REQUIRED columns are checked.
# Extra columns (new upstream additions) are intentionally allowed through.
# They will be absorbed silently by the Spark .select() firewall (Layer 2).
# This prevents quarantining legitimate file drops due to non-breaking additions.
#
EXPECTED_SCHEMAS: dict[str, dict[str, dict]] = {
    "orders": {
        "order_id":     {"parquet_type": "BYTE_ARRAY",    "required": True},
        "user_id":      {"parquet_type": "BYTE_ARRAY",    "required": True},
        "amount":       {"parquet_type": "DOUBLE",        "required": True},
        "currency":     {"parquet_type": "BYTE_ARRAY",    "required": True},
        "process_date": {"parquet_type": "INT32",         "required": True},
        "channel":      {"parquet_type": "BYTE_ARRAY",    "required": False},
        "product_sku":  {"parquet_type": "BYTE_ARRAY",    "required": False},
    },
    "events": {
        "event_id":     {"parquet_type": "BYTE_ARRAY",    "required": True},
        "user_id":      {"parquet_type": "BYTE_ARRAY",    "required": True},
        "event_type":   {"parquet_type": "BYTE_ARRAY",    "required": True},
        "timestamp":    {"parquet_type": "BYTE_ARRAY",    "required": True},
    },
    "user_segments": {
        "user_id":      {"parquet_type": "BYTE_ARRAY",    "required": True},
        "user_segment": {"parquet_type": "BYTE_ARRAY",    "required": True},
        "segment_date": {"parquet_type": "INT32",         "required": True},
    },
}


# ── File Type Detection ────────────────────────────────────────────────────────

def detect_file_type(object_name: str) -> Optional[str]:
    """
    Infer the file type from the GCS object name path.

    Args:
        object_name: GCS object path, e.g. "orders/orders_2025-06-01.parquet"

    Returns:
        One of: "orders", "events", "user_segments", or None if unrecognised.
    """
    lower = object_name.lower()
    if "orders/" in lower:
        return "orders"
    if "events/" in lower:
        return "events"
    if "segments/" in lower or "user_segments/" in lower:
        return "user_segments"
    logger.warning(f"Unrecognised file path, cannot detect file type: {object_name}")
    return None


# ── MD5 Deduplication (Firestore) ─────────────────────────────────────────────

def compute_md5(bucket_name: str, object_name: str) -> str:
    """
    Download the file content and compute its MD5 hash.

    Why MD5 and not filename?
        - The upstream team might re-drop the SAME filename with different content
          (bug fix, data correction). MD5 on content catches this: different content
          = different hash = treated as a new file.
        - The same filename with identical content = same hash = duplicate = skipped.
    """
    storage_client = storage.Client(project=PROJECT_ID)
    blob = storage_client.bucket(bucket_name).blob(object_name)
    content = blob.download_as_bytes()
    return hashlib.md5(content).hexdigest()


def is_duplicate(file_md5: str, filename: str) -> bool:
    """
    Check Firestore to determine if this file has already been processed.

    Why Firestore (not SQLite / in-memory)?
        Cloud Run is stateless and ephemeral. Multiple container instances can
        run simultaneously (e.g., five files arriving at once). Each instance has
        its own isolated memory — in-memory dicts and SQLite files are invisible
        across instances. Firestore is a globally distributed database: all
        instances share the same state. Atomic writes prevent race conditions.

    Args:
        file_md5: MD5 hex digest of the file content.
        filename: Original GCS object name (for logging only).

    Returns:
        True if this file has already been seen; False if it is new.
    """
    db = firestore.Client(project=PROJECT_ID)
    doc_ref = db.collection(FIRESTORE_COLLECTION).document(file_md5)
    doc = doc_ref.get()

    if doc.exists:
        logger.info(f"DUPLICATE DETECTED — hash {file_md5} already exists. Skipping: {filename}")
        return True

    # Atomic write: mark as 'processing' BEFORE doing any work.
    # Prevents two concurrent Cloud Run instances processing the same file simultaneously.
    doc_ref.set({
        "filename": filename,
        "status": "processing",
        "first_seen_at": firestore.SERVER_TIMESTAMP,
    })
    logger.info(f"New file registered in Firestore. Hash: {file_md5}, File: {filename}")
    return False


def update_firestore_status(file_md5: str, status: str, reason: str = "") -> None:
    """Update the Firestore record after processing completes or fails."""
    db = firestore.Client(project=PROJECT_ID)
    update_data: dict = {"status": status, "processed_at": firestore.SERVER_TIMESTAMP}
    if reason:
        update_data["failure_reason"] = reason
    db.collection(FIRESTORE_COLLECTION).document(file_md5).update(update_data)


# ── Schema Validation ─────────────────────────────────────────────────────────

def validate_schema(bucket_name: str, object_name: str, file_type: str) -> tuple[bool, str]:
    """
    Validate a Parquet file's schema against the registered data contract.

    Performance note:
        pq.read_schema() reads ONLY the Parquet file footer metadata — it does
        not download or parse the data rows. For a 150MB Parquet file, this
        operation completes in milliseconds and uses negligible memory.

    Args:
        bucket_name: GCS bucket containing the file.
        object_name: GCS object path.
        file_type: One of "orders", "events", "user_segments".

    Returns:
        (is_valid: bool, failure_reason: str)
    """
    gcs_uri = f"gs://{bucket_name}/{object_name}"
    contract = EXPECTED_SCHEMAS.get(file_type, {})

    try:
        parquet_schema = pq.read_schema(gcs_uri)
        actual_columns = {field.name for field in parquet_schema}
    except Exception as exc:
        return False, f"Could not read Parquet schema: {exc}"

    for col_name, col_spec in contract.items():
        if col_spec["required"] and col_name not in actual_columns:
            return False, f"Required column '{col_name}' missing from {file_type} file"

    logger.info(f"Schema validation PASSED for {file_type}: {object_name}")
    return True, ""


# ── File Routing ───────────────────────────────────────────────────────────────

def promote_to_validated(source_bucket: str, object_name: str) -> None:
    """Copy file from raw bucket to validated bucket."""
    storage_client = storage.Client(project=PROJECT_ID)
    source_blob = storage_client.bucket(source_bucket).blob(object_name)
    dest_bucket = storage_client.bucket(VALIDATED_BUCKET)
    source_blob.bucket.copy_blob(source_blob, dest_bucket, object_name)
    logger.info(f"Promoted to validated: {object_name}")


def route_to_quarantine(source_bucket: str, object_name: str) -> None:
    """Copy file from raw bucket to quarantine bucket (original is preserved)."""
    storage_client = storage.Client(project=PROJECT_ID)
    source_blob = storage_client.bucket(source_bucket).blob(object_name)
    dest_bucket = storage_client.bucket(QUARANTINE_BUCKET)
    source_blob.bucket.copy_blob(source_blob, dest_bucket, object_name)
    logger.warning(f"Routed to quarantine: {object_name}")


# ── Slack Alerting ─────────────────────────────────────────────────────────────

def send_slack_alert(file_name: str, file_type: str, reason: str) -> None:
    """
    Post a Slack message to the #data-ops-alerts channel.

    Design: We alert on QUARANTINE only — not on duplicate skips.
    Duplicate skips are normal operational behaviour; quarantines indicate
    an upstream data quality issue that requires human action.
    """
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return

    message = {
        "text": (
            f":warning: *File Quarantined — RetailEdge Pipeline*\n"
            f"*File:* `{file_name}`\n"
            f"*Type:* `{file_type}`\n"
            f"*Reason:* {reason}\n"
            f"_Upstream team: please fix the file and re-drop. Pipeline is blocked for this file._"
        )
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
        response.raise_for_status()
        logger.info("Slack alert sent successfully")
    except requests.RequestException as exc:
        logger.error(f"Failed to send Slack alert: {exc}")


# ── Cloud Run Entry Point ──────────────────────────────────────────────────────

@functions_framework.cloud_event
def process_gcs_event(cloud_event) -> None:
    """
    GCS event trigger handler. Invoked on every object creation in the raw bucket.

    Flow:
        1. Extract file details from the event payload
        2. Skip non-Parquet files
        3. Detect file type from path
        4. Compute MD5 and check Firestore for duplicates
        5. Validate schema against data contract
        6. Route: Validated bucket OR Quarantine + Slack alert
        7. Update Firestore with final status

    Args:
        cloud_event: GCS storage event (EventArc format).
    """
    data = cloud_event.data
    source_bucket = data["bucket"]
    object_name   = data["name"]

    logger.info(f"Processing GCS event: gs://{source_bucket}/{object_name}")

    # ── Filter: Only process Parquet files ────────────────────────────────────
    if not object_name.lower().endswith(".parquet"):
        logger.info(f"Non-Parquet file — skipping: {object_name}")
        return

    # ── Detect file type from path ─────────────────────────────────────────────
    file_type = detect_file_type(object_name)
    if file_type is None:
        logger.warning(f"Unknown file type — routing to quarantine: {object_name}")
        route_to_quarantine(source_bucket, object_name)
        send_slack_alert(object_name, "unknown", "Could not determine file type from path")
        return

    # ── Deduplication check ────────────────────────────────────────────────────
    file_md5 = compute_md5(source_bucket, object_name)
    if is_duplicate(file_md5, object_name):
        return  # Duplicate — safely skip

    # ── Schema validation ──────────────────────────────────────────────────────
    is_valid, failure_reason = validate_schema(source_bucket, object_name, file_type)

    if is_valid:
        promote_to_validated(source_bucket, object_name)
        update_firestore_status(file_md5, status="validated")
        logger.info(f"SUCCESS — {file_type} file validated and promoted: {object_name}")
    else:
        route_to_quarantine(source_bucket, object_name)
        update_firestore_status(file_md5, status="quarantined", reason=failure_reason)
        send_slack_alert(object_name, file_type, failure_reason)
        logger.error(f"QUARANTINED — {file_type} file failed validation: {object_name} | Reason: {failure_reason}")
