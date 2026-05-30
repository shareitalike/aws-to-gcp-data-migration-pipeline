"""
Cloud Composer Airflow DAG
===============================================
GCP Data Engineering Engagement

DAG: retailedge_daily_pipeline
Schedule: Daily at 00:00 IST (18:30 UTC)

Purpose:
    Orchestrates the complete AWS-to-GCP data pipeline:
      1. Transfer daily Parquet files from AWS S3 to GCS Raw
      2. Wait for Cloud Run event-driven validation to complete
      3. Trigger PySpark enrichment job on Serverless Dataproc
      4. Load enriched Parquet to BigQuery staging (WRITE_TRUNCATE)
      5. Run dbt incremental model (MERGE into production + schema tests)
      6. Send success notification

Design Decisions:
    {{ ds }} vs datetime.today():
        ALL date parameters use {{ ds }} (Airflow's logical execution date).
        NEVER datetime.today() which breaks on retry.
        Retry on March 22nd for March 19th's failed run → {{ ds }} = 2026-03-19.
        The pipeline always processes the correct business date.

    Conditional skip (ShortCircuitOperator):
        If the validation gate finds zero valid files (unusual but possible),
        downstream Spark and BigQuery tasks are SKIPPED (not FAILED).
        This avoids Spark exceptions on empty input and avoids false alerts.

    Per-task retry policy:
        Transfer: 3 retries (S3 can be slow/unavailable temporarily)
        Spark: 2 retries (Dataproc provisioning occasionally delays)
        BQ Load: 3 retries (quota limits are the most common transient failure)
        dbt: 1 retry (dbt failures are usually logical, not infrastructure)

    SLA monitoring:
        Total DAG must complete within 4 hours (pipeline SLA: data by 06:00 IST).
        Per-task SLA: 30 minutes. If any task exceeds 30 min, Slack alert fires.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
import requests

# ── Configuration ──────────────────────────────────────────────────────────────
PROJECT_ID         = Variable.get("gcp_project_id", default_var="aws-to-gcp-data-migration")
GCP_REGION         = "asia-south1"
DATAPROC_REGION    = GCP_REGION
GCS_RAW_BUCKET     = Variable.get("gcs_raw_bucket", default_var="retailedge-landing-aws-to-gcp-data-migration")
GCS_VALIDATED_BUCKET = Variable.get("gcs_validated_bucket", default_var="retailedge-landing-validated-aws-to-gcp-data-migration")
GCS_PROCESSED_BUCKET = Variable.get("gcs_processed_bucket", default_var="retailedge-processed-aws-to-gcp-data-migration")
SPARK_JOB_FILE     = f"gs://retailedge-processed-aws-to-gcp-data-migration/code/process_daily_orders.py"
SLACK_WEBHOOK_URL  = Variable.get("slack_webhook_url", default_var="")

# AWS S3 Source Configuration
AWS_CONN_ID        = "aws_retailedge"
S3_BUCKET          = "retailedge-oms-export"
S3_PREFIX          = "orders/"


# ── Slack Notification Helper ──────────────────────────────────────────────────

def send_slack_notification(context: dict, status: str) -> None:
    """Send pipeline status notification to Slack #data-ops-alerts."""
    if not SLACK_WEBHOOK_URL:
        return

    dag_id       = context["dag"].dag_id
    execution_date = context["ds"]
    run_id       = context["run_id"]

    emoji   = ":white_check_mark:" if status == "SUCCESS" else ":red_circle:"
    message = {
        "text": (
            f"{emoji} *RetailEdge Pipeline {status}*\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Date:* `{execution_date}`\n"
            f"*Run ID:* `{run_id}`"
        )
    }

    try:
        requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
    except Exception:
        pass  # Non-critical — don't fail the DAG for a Slack notification error


def on_failure_callback(context: dict) -> None:
    """Called by Airflow when any task fails."""
    send_slack_notification(context, "FAILURE")


def on_success_callback(context: dict) -> None:
    """Called by Airflow when the DAG completes successfully."""
    send_slack_notification(context, "SUCCESS")


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Called by Airflow when a task exceeds its SLA."""
    if SLACK_WEBHOOK_URL:
        message = {
            "text": (
                f":clock1: *SLA Miss — RetailEdge Pipeline*\n"
                f"Tasks exceeding 30-minute SLA: `{[t.task_id for t in blocking_tis]}`"
            )
        }
        try:
            requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
        except Exception:
            pass


# ── Default Task Arguments ─────────────────────────────────────────────────────
default_args: dict[str, Any] = {
    "owner":              "retailedge-data-team",
    "depends_on_past":    False,
    "start_date":         days_ago(1),
    "email_on_failure":   False,
    "email_on_retry":     False,
    "retries":            2,
    "retry_delay":        timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_callback,
    "sla":                timedelta(minutes=30),
}


# ── DAG Definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="retailedge_daily_pipeline",
    default_args=default_args,
    description=(
        "RetailEdge Global — AWS S3 to BigQuery daily data pipeline. "
        "Medallion architecture: Raw → Validated → Processed → Warehouse."
    ),
    schedule_interval="30 18 * * *",   # 18:30 UTC = 00:00 IST
    catchup=False,
    max_active_runs=1,                 # Prevent concurrent runs for the same pipeline
    tags=["retailedge", "daily", "production"],
    on_success_callback=on_success_callback,
    sla_miss_callback=sla_miss_callback,
) as dag:

    # ── Task 1: Transfer S3 → GCS Raw ─────────────────────────────────────────
    transfer_s3_to_gcs = S3ToGCSOperator(
        task_id="transfer_s3_to_gcs",
        aws_conn_id=AWS_CONN_ID,
        bucket=S3_BUCKET,
        prefix=f"orders/orders_{{{{ ds }}}}.parquet",
        dest_gcs=f"gs://{GCS_RAW_BUCKET}/",
        replace=True,                  # Idempotent: GCS overwrite = same bytes
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # ── Task 2: Wait for Cloud Run validation to complete ─────────────────────
    # Cloud Run fires asynchronously on GCS event. We wait for the validated file
    # to appear before triggering Spark. Timeout: 60 minutes.
    wait_for_validated_file = GCSObjectExistenceSensor(
        task_id="wait_for_validated_file",
        bucket=GCS_VALIDATED_BUCKET,
        object=f"orders/orders_{{{{ ds }}}}.parquet",
        timeout=3600,                  # 60 minutes
        poke_interval=60,              # Check every 60 seconds
        mode="reschedule",             # Release worker slot while waiting
    )

    # ── Task 3: Check that at least one valid file exists (conditional skip) ──
    def check_any_valid_files(**context) -> bool:
        """
        Return False if no valid files were found for this date.
        ShortCircuitOperator will SKIP all downstream tasks if this returns False.
        This prevents Spark from starting with empty input (which throws exceptions).
        """
        from google.cloud import storage
        gcs = storage.Client()
        bucket = gcs.bucket(GCS_VALIDATED_BUCKET)
        blobs  = list(bucket.list_blobs(prefix=f"orders/orders_{context['ds']}"))
        if not blobs:
            context["task_instance"].xcom_push(key="skip_reason", value="no_valid_files")
            return False
        return True

    check_valid_files = ShortCircuitOperator(
        task_id="check_valid_files",
        python_callable=check_any_valid_files,
    )

    # ── Task 4: PySpark Enrichment (Serverless Dataproc) ──────────────────────
    trigger_dataproc_spark = DataprocSubmitJobOperator(
        task_id="trigger_dataproc_spark",
        project_id=PROJECT_ID,
        region=DATAPROC_REGION,
        job={
            "reference":  {"project_id": PROJECT_ID},
            "placement":  {"cluster_name": ""},   # Empty = Serverless (no persistent cluster)
            "pyspark_job": {
                "main_python_file_uri": SPARK_JOB_FILE,
                "args": [
                    "--date", "{{ ds }}",          # ALWAYS {{ ds }}, NEVER datetime.today()
                    "--env", "prod",
                ],
                "properties": {
                    "spark.sql.sources.partitionOverwriteMode": "dynamic",
                    "spark.sql.adaptive.enabled": "true",
                },
            },
        },
        retries=2,
        retry_delay=timedelta(minutes=10),
    )

    # ── Task 5: Load BigQuery Staging (WRITE_TRUNCATE) ─────────────────────────
    load_bq_staging = BigQueryInsertJobOperator(
        task_id="load_bq_staging",
        project_id=PROJECT_ID,
        location="asia-south1",          # ← required: dataset lives in asia-south1, not US
        configuration={
            "load": {
                "sourceUris":    [f"{GCS_PROCESSED_BUCKET}/enriched_orders/process_date={{{{ ds }}}}/*.parquet"],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": "staging",
                    "tableId":   "orders_daily",
                },
                "sourceFormat":       "PARQUET",
                "writeDisposition":   "WRITE_TRUNCATE",  # Idempotent wipe + reload
                "autodetect":         True,
            }
        },
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # ── Task 6: dbt Incremental Run (MERGE + Schema Tests) ────────────────────
    run_dbt_incremental = KubernetesPodOperator(
        task_id="run_dbt_incremental",
        name="dbt-run-retailedge",
        namespace="airflow",
        image=f"asia-south1-docker.pkg.dev/{PROJECT_ID}/retailedge/dbt-runner:latest",
        cmds=["bash", "-c"],
        arguments=[
            # dbt run: MERGE staging → production with on_schema_change=append_new_columns
            # dbt test: circuit breaker — fails DAG if any quality test fails
            f"dbt run --target prod --vars '{{execution_date: {{{{ ds }}}}}}' "
            f"--select enriched_orders && "
            f"dbt test --target prod --select enriched_orders"
        ],
        env_vars={
            "DBT_PROFILES_DIR": "/dbt",
            "GCP_PROJECT_ID":   PROJECT_ID,
        },
        service_account_name="dbt-sa",
        get_logs=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # ── Task Dependencies (the DAG) ────────────────────────────────────────────
    #
    # transfer_s3_to_gcs
    #   → wait_for_validated_file
    #     → check_valid_files
    #       → trigger_dataproc_spark    (SKIPPED if no valid files)
    #         → load_bq_staging         (SKIPPED if no valid files)
    #           → run_dbt_incremental   (SKIPPED if no valid files)
    #
    (
        transfer_s3_to_gcs
        >> wait_for_validated_file
        >> check_valid_files
        >> trigger_dataproc_spark
        >> load_bq_staging
        >> run_dbt_incremental
    )
