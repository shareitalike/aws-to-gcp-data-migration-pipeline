"""
RetailEdge Global — Historical Data Backfill DAG
=================================================
Purpose:
    Orchestrates the massive 18-month historical backfill (Jan 2022 to June 2023)
    from AWS S3 to GCP using PySpark on Dataproc.

Design Decisions (The "12-Day Detail"):
    - max_active_runs=12: Prevents the backfill from spinning up hundreds of 
      concurrent Dataproc jobs and crashing the cluster or hitting AWS API rate limits.
      It throttles the backfill to process exactly 12 days at a time.
    - execution_date ({{ ds }}): The DAG relies on Airflow's logical execution date 
      to dynamically point the Spark job to the specific daily folder in S3.

Usage:
    Do NOT turn this DAG on in the UI. It is meant to be triggered via CLI for specific ranges:
    airflow dags backfill -s 2022-01-01 -e 2023-06-30 retailedge_historical_backfill
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

# Environment variables (typically pulled from Airflow Variables)
PROJECT_ID = "retailedge-prod"
REGION = "us-central1"
CLUSTER_NAME = "retailedge-ephemeral-cluster"
PYSPARK_URI = "gs://retailedge-code-prod/scripts/migrate_historical_history.py"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="retailedge_historical_backfill",
    default_args=default_args,
    description="Chunked 18-month historical backfill from S3 to GCS",
    # Set a start date far in the past to allow backfilling
    start_date=datetime(2022, 1, 1),
    # Run daily (so each DAG run processes exactly 1 day of data)
    schedule_interval="@daily",
    catchup=False, # We keep this False in code, and explicitly use the CLI `backfill` command
    
    # THE KEY PERFORMANCE POINT (The 12-Day Detail)
    max_active_runs=12,
    concurrency=12,
    
    tags=["historical", "pyspark", "migration"],
) as dag:

    # 1. Dataproc Job Configuration
    # We use Airflow's Jinja templating ({{ ds }}) to inject the logical date.
    # E.g., On the run for Jan 1st 2022, {{ ds }} evaluates to "2022-01-01"
    
    pyspark_job_config = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            "args": [
                # Dynamically point to the specific day's folder in S3
                "--s3_input", f"s3a://retailedge-oms-export/orders/date={{{{ ds }}}}/",
                # Write to the specific day's folder in GCS
                "--gcs_output", f"gs://retailedge-processed-prod/enriched_orders/"
            ]
        }
    }

    # 2. Submit the Job to Dataproc
    run_historical_migration = DataprocSubmitJobOperator(
        task_id="run_pyspark_historical_migration",
        job=pyspark_job_config,
        region=REGION,
        project_id=PROJECT_ID
    )

    run_historical_migration
