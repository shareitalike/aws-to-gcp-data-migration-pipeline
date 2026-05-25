"""
RetailEdge Global — BigQuery Staging Load & Production MERGE
=============================================================
Cloud Data Architecture Group | GCP Data Engineering Engagement

Purpose:
    1. load_staging(): Load date-partitioned Parquet from GCS Processed into
       BigQuery staging table using WRITE_TRUNCATE (idempotent wipe + reload)
    2. merge_production(): Execute the MERGE from staging into the production
       analytics table (upsert semantics by order_id)

Idempotency Design:
    - Staging uses WRITE_TRUNCATE: every run wipes and reloads staging.
      Running 3 times = same staging table. No accumulation.
    - Production uses MERGE on order_id: matched rows UPDATE (same values
      if re-run), unmatched rows INSERT once. Running 10 times = same table.
    - Both operations include the partition date to enable partition pruning
      in BigQuery — reduces bytes scanned during nightly MERGE significantly.
"""

import argparse
import logging
import os
import sys
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("retailedge.warehouse")

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID        = os.environ.get("GCP_PROJECT_ID", "retailedge-data-prod")
PROCESSED_BUCKET  = os.environ.get("PROCESSED_BUCKET", "gs://retailedge-processed-prod")
STAGING_DATASET   = "staging"
STAGING_TABLE     = "orders_daily"
PRODUCTION_DATASET = "core"
PRODUCTION_TABLE  = "enriched_orders"


# ── Step 1: BigQuery Staging Load ─────────────────────────────────────────────

def load_staging(execution_date: str) -> None:
    """
    Load date-partitioned Parquet files from GCS Processed into BigQuery staging.

    Design Decisions:
        WRITE_TRUNCATE:
            The staging table is EPHEMERAL. It exists only to feed the nightly MERGE.
            We wipe it completely and reload from scratch on every run.
            Running this 3 times for the same date = same staging table.
            This is idempotency via full replacement.

        autodetect=True:
            Used because Spark normalised all column types before writing the Parquet
            (see cast_and_select() in process_daily_orders.py). The Parquet schema
            is our own controlled output — not raw upstream data.
            Autodetect reliably reads our Parquet schema metadata from the file footer.

        Source URI pattern:
            Loads ONLY files from the specific date's partition folder.
            Uses /* wildcard to handle multi-part Parquet output from Spark.

    Args:
        execution_date: Date in YYYY-MM-DD format (Airflow {{ ds }}).
    """
    client = bigquery.Client(project=PROJECT_ID)

    source_uri      = f"{PROCESSED_BUCKET}/enriched_orders/process_date={execution_date}/*.parquet"
    destination_ref = f"{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}"

    logger.info(f"Loading staging table from: {source_uri}")
    logger.info(f"Destination: {destination_ref}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    try:
        load_job = client.load_table_from_uri(
            source_uris=source_uri,
            destination=destination_ref,
            job_config=job_config,
        )

        logger.info(f"Load job submitted: {load_job.job_id}")
        load_job.result()  # Wait for completion

        destination_table = client.get_table(destination_ref)
        logger.info(
            f"Staging load COMPLETE — "
            f"{destination_table.num_rows:,} rows in {destination_ref}"
        )

    except GoogleCloudError as exc:
        logger.error(f"BigQuery load job FAILED: {exc}")
        raise


# ── Step 2: Production MERGE ───────────────────────────────────────────────────

def merge_production(execution_date: str) -> None:
    """
    Execute a MERGE from staging into the production analytics table.

    Idempotency via MERGE semantics:
        WHEN MATCHED: UPDATE the existing row to the new values.
            If re-run: updates to the SAME values — no net change.
        WHEN NOT MATCHED: INSERT the new row.
            If re-run: row already exists → falls into MATCHED branch.
        Running this 10 times = same production table as running it once.

    Partition pruning in MERGE:
        The ON clause includes BOTH order_id AND process_date.
        Including process_date is NOT required for correctness (order_id is unique).
        It is required for COST EFFICIENCY:
            Without process_date in ON: BigQuery scans the full production table
                (~500GB) to find matching order_ids → slow and expensive.
            With process_date in ON: BigQuery scans ONLY the process_date=date
                partition (~700MB) → 99.86% cost reduction per nightly MERGE.

    Args:
        execution_date: Date in YYYY-MM-DD format (Airflow {{ ds }}).
    """
    client = bigquery.Client(project=PROJECT_ID)

    target_table = f"`{PROJECT_ID}.{PRODUCTION_DATASET}.{PRODUCTION_TABLE}`"
    source_table = f"`{PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`"

    merge_sql = f"""
    MERGE INTO {target_table} AS target
    USING (
        SELECT *
        FROM {source_table}
        WHERE process_date = '{execution_date}'
    ) AS source

    ON target.order_id     = source.order_id
    AND target.process_date = source.process_date  -- For partition pruning (not correctness)

    WHEN MATCHED THEN
        UPDATE SET
            target.amount        = source.amount,
            target.currency      = source.currency,
            target.user_segment  = source.user_segment,
            target.event_count   = source.event_count,
            target.event_types   = source.event_types,
            target.updated_at    = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN
        INSERT (
            order_id,
            user_id,
            amount,
            currency,
            process_date,
            user_segment,
            event_count,
            event_types,
            inserted_at,
            updated_at
        )
        VALUES (
            source.order_id,
            source.user_id,
            source.amount,
            source.currency,
            source.process_date,
            source.user_segment,
            source.event_count,
            source.event_types,
            CURRENT_TIMESTAMP(),
            CURRENT_TIMESTAMP()
        );
    """

    logger.info(f"Executing MERGE into {target_table} for date={execution_date}")

    try:
        query_job = client.query(merge_sql)
        query_job.result()  # Wait for completion

        logger.info(
            f"MERGE COMPLETE — "
            f"Rows affected: {query_job.num_dml_affected_rows:,} | "
            f"Bytes processed: {query_job.total_bytes_processed / (1024**3):.2f} GB | "
            f"Job: {query_job.job_id}"
        )

    except GoogleCloudError as exc:
        logger.error(f"BigQuery MERGE FAILED: {exc}")
        raise


# ── Main ───────────────────────────────────────────────────────────────────────

def main(execution_date: str, step: str) -> None:
    """
    Entry point — run staging load, production MERGE, or both.

    Args:
        execution_date: Processing date in YYYY-MM-DD format.
        step: Which step to run — "load", "merge", or "all".
    """
    logger.info(f"Starting warehouse operation | date={execution_date} | step={step}")

    try:
        if step in ("load", "all"):
            load_staging(execution_date)

        if step in ("merge", "all"):
            merge_production(execution_date)

        logger.info(f"Warehouse operation COMPLETE | date={execution_date} | step={step}")

    except Exception as exc:
        logger.error(f"Warehouse operation FAILED: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RetailEdge Warehouse Load & Merge")
    parser.add_argument(
        "--date",
        required=True,
        type=str,
        help="Execution date YYYY-MM-DD (from Airflow {{ ds }}).",
    )
    parser.add_argument(
        "--step",
        choices=["load", "merge", "all"],
        default="all",
        help="Which warehouse step to execute.",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Expected YYYY-MM-DD.")
        sys.exit(1)

    main(execution_date=args.date, step=args.step)
