"""
RetailEdge Global — Historical Data Migration (PySpark CSV-to-Parquet)
======================================================================
Cloud Data Architecture Group | GCP Data Engineering Engagement

Purpose:
    One-time backfill script to migrate and convert historical retail orders
    from legacy uncompressed AWS S3 CSV format into optimized, Snappy-compressed,
    date-partitioned Parquet files on GCS.

Parameters:
    --s3_input: S3 input prefix or bucket URL (e.g. s3a://retailedge-oms-export/orders/)
    --gcs_output: GCS output bucket URL (e.g. gs://retailedge-processed-prod/enriched_orders/)

Design & Performance Decisions:
    - KryoSerializer: Used for fast, low-memory object serialization.
    - Explicit Type Casting: Normalizes all raw CSV fields (strings) into strict target data types
      to match BigQuery warehouse schema contracts (DOUBLE, INT32, DATE).
    - Data Quality Filters: Removes negative amounts and orphaned order/user IDs.
    - Dynamic Partitioning: Writes partitioned Parquet files by 'process_date' using Snappy compression.
"""

import sys
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("retailedge.historical_migration")

def main(s3_input_path, gcs_output_path):
    # 1. Initialize Spark Session with KryoSerializer and Dynamic Partitioning
    spark = SparkSession.builder \
        .appName("RetailEdge-Historical-CSV-to-Parquet-Migration") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
        
    logger.info(f"Starting historical migration job.")
    logger.info(f"Source (AWS S3 CSV): {s3_input_path}")
    logger.info(f"Destination (GCS Parquet): {gcs_output_path}")
    
    # 2. Read legacy CSV files from S3/raw input
    # Enforces header=True and treats all raw fields as string
    logger.info("Reading raw CSV files...")
    df_raw_csv = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(s3_input_path)
        
    # 3. Enforce Data Contract Schemas (Explicit Type Casting)
    logger.info("Enforcing schemas and casting data types...")
    df_normalized = df_raw_csv.select(
        col("order_id").cast("string"),
        col("user_id").cast("string"),
        col("amount").cast("double"),
        col("currency").cast("string"),
        col("channel").cast("string"),
        col("product_sku").cast("string"),
        # Enforce date format YYYY-MM-DD for partition structure
        to_date(col("process_date"), "yyyy-MM-dd").alias("process_date")
    )
    
    # 4. Data Quality Filtering
    # Drop records with null primary keys or negative values (system anomalies)
    logger.info("Applying data quality filters (dropping negative amounts and null IDs)...")
    df_clean = df_normalized.filter(
        (col("order_id").isNotNull()) & 
        (col("user_id").isNotNull()) & 
        (col("amount") > 0)
    )
    
    # 5. Write to GCS (Snappy compression, partitioned by process_date)
    logger.info(f"Writing Snappy-compressed Parquet files to GCS partitioned by process_date...")
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("process_date") \
        .option("compression", "snappy") \
        .parquet(gcs_output_path)
        
    logger.info("Historical migration completed successfully!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Legacy CSV to Parquet Migration PySpark Job")
    parser.add_argument("--s3_input", required=True, help="S3 input prefix or bucket URL containing raw CSVs")
    parser.add_argument("--gcs_output", required=True, help="GCS output bucket URL for processed Parquet")
    args = parser.parse_args()
    
    main(s3_input_path=args.s3_input, gcs_output_path=args.gcs_output)
