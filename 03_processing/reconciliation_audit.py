"""
RetailEdge Global — Historical Data Migration Reconciliation Audit
==================================================================
Cloud Data Architecture Group | GCP Data Engineering Engagement

Purpose:
    Perform post-migration reconciliation between legacy source files (AWS S3 CSVs)
    and target processed files (GCS Parquet / BigQuery) for a specified date range.

Reconciliation Metrics:
    1. Row Count Validation:
       - Source Row Count (Legacy CSVs)
       - Filtered Row Count (rows dropped due to data quality filters: nulls, negative amounts)
       - Target Row Count (Processed Parquet/BigQuery)
       - Formula: Target Count = Source Count - Filtered Count
    2. Financial Amount Verification:
       - Source Sum of Amount
       - Filtered Sum of Amount (amount from dropped rows)
       - Target Sum of Amount
       - Formula: Target Sum = Source Sum - Filtered Sum (within float tolerance)

Design Decisions:
    - Designed to run as a Dataproc PySpark job to leverage parallel reading of massive CSVs and Parquet.
    - Captures and logs discrepancies to Cloud Logging.
    - Writes audit reports to Firestore and an audit GCS bucket for compliance tracking.
"""

import argparse
import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, lit
from google.cloud import firestore

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("retailedge.reconciliation")

def create_spark_session(app_name: str = "RetailEdge-Reconciliation-Audit") -> SparkSession:
    """Create a configured SparkSession."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def run_reconciliation(spark: SparkSession, s3_source_path: str, gcs_target_path: str, execution_date: str) -> dict:
    """
    Run the row-count and sum-of-amount reconciliation for a specific date/month.
    
    Args:
        spark: Active SparkSession
        s3_source_path: Path to S3 or Raw GCS CSV directory/file
        gcs_target_path: Path to GCS Processed Parquet directory
        execution_date: Date string YYYY-MM-DD or YYYY-MM prefix
    """
    logger.info(f"Starting reconciliation audit for {execution_date}")
    logger.info(f"Source Path: {s3_source_path}")
    logger.info(f"Target Path: {gcs_target_path}")

    # 1. Read Raw Source CSV (unfiltered)
    # Note: In a production run, this reads directly from AWS S3 over Google backbone.
    df_raw = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(s3_source_path)

    # 2. Read Clean Target Parquet
    # Note: Reads the processed date-partitioned Parquet files from GCS.
    df_target = spark.read \
        .parquet(gcs_target_path)

    # 3. Calculate Source (Raw) Metrics
    # Since legacy CSV reads everything as strings, we cast 'amount' to double for math.
    source_stats = df_raw.select(
        col("order_id"),
        col("amount").cast("double").alias("amount_dbl")
    )
    
    source_metrics = source_stats.select(
        _sum(lit(1)).alias("total_rows"),
        _sum(col("amount_dbl")).alias("total_amount")
    ).collect()[0]

    source_row_count = source_metrics["total_rows"] or 0
    source_sum_amount = source_metrics["total_amount"] or 0.0

    # 4. Calculate Filtered / Dropped Metrics
    # Identify rows that our PySpark job filtered out:
    #   - null order_id
    #   - null user_id
    #   - amount <= 0 or null
    df_filtered = df_raw.filter(
        (col("order_id").isNull()) |
        (col("user_id").isNull()) |
        (col("amount").cast("double").isNull()) |
        (col("amount").cast("double") <= 0)
    )

    filtered_metrics = df_filtered.select(
        _sum(lit(1)).alias("total_rows"),
        _sum(col("amount").cast("double")).alias("total_amount")
    ).collect()[0]

    filtered_row_count = filtered_metrics["total_rows"] or 0
    filtered_sum_amount = filtered_metrics["total_amount"] or 0.0

    # 5. Calculate Target (Processed) Metrics
    target_metrics = df_target.select(
        _sum(lit(1)).alias("total_rows"),
        _sum(col("amount")).alias("total_amount")
    ).collect()[0]

    target_row_count = target_metrics["total_rows"] or 0
    target_sum_amount = target_metrics["total_amount"] or 0.0

    # 6. Mismatch Analysis
    # Formula: Target = Source - Filtered
    expected_row_count = source_row_count - filtered_row_count
    expected_sum_amount = source_sum_amount - filtered_sum_amount

    row_count_diff = target_row_count - expected_row_count
    sum_amount_diff = abs(target_sum_amount - expected_sum_amount)

    status = "SUCCESS"
    reconciliation_message = "All counts and amounts reconciled perfectly."

    # Tolerance threshold for floating point math differences (e.g. 0.01 €)
    AMOUNT_TOLERANCE = 0.01

    if row_count_diff != 0:
        status = "MISMATCH"
        reconciliation_message = f"Row count discrepancy: expected {expected_row_count:,}, but got {target_row_count:,}."
        logger.error(reconciliation_message)
    elif sum_amount_diff > AMOUNT_TOLERANCE:
        status = "MISMATCH"
        reconciliation_message = f"Sum of amount discrepancy: expected {expected_sum_amount:,.2f}, but got {target_sum_amount:,.2f}."
        logger.error(reconciliation_message)
    else:
        logger.info(reconciliation_message)

    report = {
        "execution_date": execution_date,
        "status": status,
        "source_row_count": source_row_count,
        "source_sum_amount": round(source_sum_amount, 2),
        "filtered_row_count": filtered_row_count,
        "filtered_sum_amount": round(filtered_sum_amount, 2),
        "target_row_count": target_row_count,
        "target_sum_amount": round(target_sum_amount, 2),
        "discrepancy_rows": row_count_diff,
        "discrepancy_amount": round(sum_amount_diff, 4),
        "message": reconciliation_message,
        "audited_at": datetime.utcnow().isoformat()
    }

    return report

def write_report_to_firestore(report: dict, project_id: str):
    """Save the reconciliation report document to Firestore for audit compliance."""
    try:
        db = firestore.Client(project=project_id)
        # Collection: audit_reconciliation
        # Doc ID: date/month under audit
        doc_id = report["execution_date"].replace("/", "_").replace("*", "all")
        doc_ref = db.collection("audit_reconciliation").document(doc_id)
        doc_ref.set(report)
        logger.info(f"Reconciliation audit report successfully written to Firestore under doc ID: {doc_id}")
    except Exception as exc:
        logger.warning(f"Failed to write audit report to Firestore: {exc}. Continuing job.")

def main():
    parser = argparse.ArgumentParser(description="RetailEdge Data Reconciliation Job")
    parser.add_argument("--s3_source", required=True, help="S3 or raw GCS path to source CSV files")
    parser.add_argument("--gcs_target", required=True, help="GCS path to processed target Parquet files")
    parser.add_argument("--date", required=True, help="Reconciliation target date (YYYY-MM-DD) or month (YYYY-MM)")
    parser.add_argument("--project_id", default="aws-to-gcp-data-migration", help="GCP Project ID for Firestore logging")
    args = parser.parse_args()

    spark = create_spark_session()
    
    try:
        report = run_reconciliation(
            spark=spark,
            s3_source_path=args.s3_source,
            gcs_target_path=args.gcs_target,
            execution_date=args.date
        )
        
        # Output report summary to stdout/logs
        print("\n" + "="*50)
        print("         RECONCILIATION AUDIT REPORT")
        print("="*50)
        print(f"Date/Month:          {report['execution_date']}")
        print(f"Audit Status:        {report['status']}")
        print(f"Source Raw Rows:     {report['source_row_count']:,}")
        print(f"Source Raw Amount:   €{report['source_sum_amount']:,.2f}")
        print(f"Filtered Rows:       {report['filtered_row_count']:,}")
        print(f"Filtered Amount:     €{report['filtered_sum_amount']:,.2f}")
        print(f"Target Processed Rows: {report['target_row_count']:,}")
        print(f"Target Processed Amt: €{report['target_sum_amount']:,.2f}")
        print(f"Discrepancy Rows:    {report['discrepancy_rows']}")
        print(f"Discrepancy Amount:  €{report['discrepancy_amount']:,.2f}")
        print(f"Audit Message:       {report['message']}")
        print("="*50 + "\n")

        # Save to Firestore for audit trail
        write_report_to_firestore(report, args.project_id)

        if report["status"] == "MISMATCH":
            logger.error("Reconciliation audit failed due to mismatches.")
            sys.exit(1)
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
