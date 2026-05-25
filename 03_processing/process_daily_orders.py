"""
RetailEdge Global — PySpark Daily Enrichment Job
=================================================
Cloud Data Architecture Group | GCP Data Engineering Engagement

Purpose:
    Reads validated daily Parquet files from GCS, performs multi-source
    enrichment (orders + events + user segments), applies business rules,
    and writes date-partitioned Parquet output to GCS Processed bucket.

Design Decisions:
    - Pre-aggregate events BEFORE join to prevent row explosion
    - Broadcast join for user_segments (dimension table, ~200MB)
    - Explicit .select() at output — Layer 2 schema drift defense
    - partitionOverwriteMode=dynamic — idempotent daily re-runs
    - Row count quality gate — circuit breaker for unexpected data expansion
    - All date parameters via --date CLI argument (Airflow {{ ds }}) — never
      datetime.today() which breaks on retry

Usage:
    python process_daily_orders.py --date 2025-06-01 --env prod
"""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType, DoubleType, DateType, IntegerType

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("retailedge.enrichment")

# ── Configuration ──────────────────────────────────────────────────────────────
CONFIGS = {
    "prod": {
        "validated_bucket": "gs://retailedge-landing-validated-aws-to-gcp-data-migration",
        "processed_bucket": "gs://retailedge-processed-aws-to-gcp-data-migration",
    },
    "dev": {
        "validated_bucket": "gs://retailedge-landing-validated-aws-to-gcp-data-migration",
        "processed_bucket": "gs://retailedge-processed-aws-to-gcp-data-migration",
    },
}

# ── Approved Output Schema ─────────────────────────────────────────────────────
#
# This list is the Layer 2 schema drift defense.
#
# ONLY columns in this list are written to the output Parquet.
# Any column added by the upstream OMS team that is NOT in this list
# will be silently dropped by the final .select() below.
# They cannot reach GCS Processed, BigQuery staging, or production
# until they are formally approved and added to this list.
#
APPROVED_OUTPUT_COLUMNS = [
    "order_id",
    "user_id",
    "amount",
    "currency",
    "process_date",
    "user_segment",
    "event_count",
    "event_types",
    # When a new column is formally approved by the data contract review:
    # add it here. Example: "promo_code"
]

# Row count expansion threshold — if output > input × threshold, raise exception
ROW_COUNT_EXPANSION_THRESHOLD = 1.10


def create_spark_session(app_name: str = "RetailEdge-DailyEnrichment") -> SparkSession:
    """
    Create a configured SparkSession for the enrichment job.

    Key configurations:
        - partitionOverwriteMode=dynamic: Only overwrite the specific date
          partition being processed. Critical for idempotency — re-running
          for June 1st must not touch June 2nd, June 3rd, etc.
        - adaptive.enabled=True: AQE automatically coalesces small shuffle
          partitions and handles data skew at runtime.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Idempotency: overwrite only the current date's partition
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # AQE: runtime optimisation for shuffle partitions and skew
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Broadcast join threshold: explicitly set for reproducible behaviour
        # beyond this size, Spark falls back to sort-merge join automatically
        .config("spark.sql.autoBroadcastJoinThreshold", str(256 * 1024 * 1024))  # 256MB
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created. Spark version: {spark.version}")
    return spark


# ── Data Reading ───────────────────────────────────────────────────────────────

def read_orders(spark: SparkSession, validated_bucket: str, date: str) -> DataFrame:
    """
    Read the daily orders Parquet file from the GCS Validated bucket.

    Args:
        spark: Active SparkSession.
        validated_bucket: GCS bucket URI (no trailing slash).
        date: Processing date in YYYY-MM-DD format.

    Returns:
        DataFrame with raw orders for the specified date.
    """
    path = f"{validated_bucket}/orders/orders_{date}.parquet"
    logger.info(f"Reading orders from: {path}")
    df = spark.read.parquet(path)
    logger.info(f"Orders loaded: {df.count():,} rows")
    return df


def read_events(spark: SparkSession, validated_bucket: str, date: str) -> DataFrame:
    """Read the daily user events Parquet file."""
    path = f"{validated_bucket}/events/events_{date}.parquet"
    logger.info(f"Reading events from: {path}")
    df = spark.read.parquet(path)
    logger.info(f"Events loaded: {df.count():,} rows")
    return df


def read_segments(spark: SparkSession, validated_bucket: str) -> DataFrame:
    """
    Read the user segments dimension table.
    Segments are refreshed weekly (every Sunday) — not daily.
    We always read the latest available version from the bucket.
    """
    path = f"{validated_bucket}/segments/"
    logger.info(f"Reading user segments from: {path}")
    df = spark.read.parquet(path)
    # Rename the source column 'segment' to the expected 'user_segment' for downstream joins
    df = df.withColumnRenamed("segment", "user_segment")
    logger.info(f"User segments loaded: {df.count():,} rows (renamed column) ")
    return df


# ── Transformations ────────────────────────────────────────────────────────────

def deduplicate_orders(df_orders: DataFrame) -> DataFrame:
    """
    Deduplicate orders by order_id, keeping the latest record per ID.

    Why dedup is needed:
        The OMS system sometimes re-transmits modified orders (e.g., amount
        corrections, status updates). If both the original and the modified
        record land in the same daily file, we want only the latest version.

    Dedup strategy:
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY process_date DESC).
        Keep only rank = 1.
    """
    logger.info("Deduplicating orders by order_id...")
    window_spec = Window.partitionBy("order_id").orderBy(F.col("process_date").desc())
    df_deduped = (
        df_orders
        .withColumn("_row_num", F.row_number().over(window_spec))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    count_before = df_orders.count()
    count_after  = df_deduped.count()
    duplicates   = count_before - count_after
    logger.info(f"Dedup complete: {count_before:,} → {count_after:,} rows ({duplicates:,} duplicates removed)")
    return df_deduped


def aggregate_events(df_events: DataFrame) -> DataFrame:
    """
    Aggregate events by user_id BEFORE joining with orders.

    Why this is critical — the row explosion problem:
        If we joined orders with raw events on user_id, and a user had 50 events,
        every one of their orders would be replicated 50 times in the result.
        100K orders × 50 events/user = 5M rows — completely wrong.

    Solution:
        Aggregate events to one row per user FIRST:
          - event_count: total events in the period
          - event_types: unique event type labels seen for this user

        Then join the aggregated events (1 row per user) with orders.
        One order + one event summary = one output row. Zero explosion.
    """
    logger.info("Aggregating events by user_id (row explosion prevention)...")
    df_events_agg = df_events.groupBy("user_id").agg(
        F.count("event_type").alias("event_count"),
        F.collect_set("event_type").alias("event_types"),
    )
    logger.info(f"Events aggregated: {df_events_agg.count():,} unique users with events")
    return df_events_agg


def enrich_orders(
    df_orders: DataFrame,
    df_events_agg: DataFrame,
    df_segments: DataFrame,
) -> DataFrame:
    """
    Join orders with aggregated events and user segments.

    Join strategy:
        - events_agg: LEFT join (orders may have users with no events)
        - segments: LEFT join with BROADCAST hint

    Broadcast join rationale:
        user_segments is ~200MB — small enough to fit in executor memory.
        Broadcasting it eliminates the shuffle entirely for this join.
        Without broadcast: 4–5 minute shuffle join.
        With broadcast: ~30 seconds. 10x speedup for this join alone.

    Args:
        df_orders: Deduplicated orders DataFrame.
        df_events_agg: Events aggregated to one row per user.
        df_segments: User segment dimension table.

    Returns:
        Enriched DataFrame with segment and event columns added to orders.
    """
    logger.info("Enriching orders with events and segments...")

    # Step 1: Join orders with aggregated events (no broadcast — events can be large)
    df_with_events = df_orders.join(df_events_agg, on="user_id", how="left")

    # Step 2: Broadcast join with segments (small dimension table)
    df_enriched = df_with_events.join(
        F.broadcast(df_segments.select("user_id", "user_segment")),
        on="user_id",
        how="left",
    )

    logger.info("Enrichment join complete")
    return df_enriched


def apply_business_rules(df_enriched: DataFrame) -> DataFrame:
    """
    Apply business-level filters and derived columns.

    Rules:
        1. Remove zero-amount transactions: These are system artifacts
           (cancellations, reversals) that should not appear in analytics.
        2. Fill NULL event_count with 0: Users with no events should show 0,
           not NULL — simplifies downstream analytics.
        3. Fill NULL user_segment: Classify unmatched users as 'unclassified'.
    """
    logger.info("Applying business rules...")
    df_cleaned = (
        df_enriched
        .filter(F.col("amount") > 0)
        .withColumn("event_count", F.coalesce(F.col("event_count"), F.lit(0)))
        .withColumn("user_segment", F.coalesce(F.col("user_segment"), F.lit("unclassified")))
    )
    return df_cleaned


def cast_and_select(df_cleaned: DataFrame) -> DataFrame:
    """
    Cast all columns to their canonical types and select ONLY approved columns.

    Type casting rationale:
        Parquet is self-describing, but upstream systems sometimes change types
        subtly (e.g., int date → string date). Explicit casting in Spark ensures
        the output Parquet always has normalised types — regardless of what types
        BigQuery autodetect reads from raw upstream files.

    .select() — Layer 2 Schema Drift Defense:
        ONLY the columns listed in APPROVED_OUTPUT_COLUMNS are written to output.
        Any extra column the upstream team added (and that passed Layer 1 validation)
        is silently dropped here. It cannot reach BigQuery staging or production
        until it is formally approved and added to APPROVED_OUTPUT_COLUMNS.
    """
    logger.info("Casting types and applying output schema filter...")

    df_typed = (
        df_cleaned
        .withColumn("order_id",     F.col("order_id").cast(StringType()))
        .withColumn("user_id",      F.col("user_id").cast(StringType()))
        .withColumn("amount",       F.col("amount").cast(DoubleType()))
        .withColumn("currency",     F.col("currency").cast(StringType()))
        .withColumn("process_date", F.col("process_date").cast(DateType()))
        .withColumn("user_segment", F.col("user_segment").cast(StringType()))
        .withColumn("event_count",  F.col("event_count").cast(IntegerType()))
    )

    # Apply approved output column filter (Layer 2 schema drift defense)
    df_final = df_typed.select(
        *[F.col(c) for c in APPROVED_OUTPUT_COLUMNS if c in df_typed.columns]
    )

    logger.info(f"Output schema: {[f.name for f in df_final.schema.fields]}")
    return df_final


# ── Quality Gate ───────────────────────────────────────────────────────────────

def assert_no_row_explosion(
    df_input: DataFrame,
    df_output: DataFrame,
    threshold: float = ROW_COUNT_EXPANSION_THRESHOLD,
) -> None:
    """
    Assert that the output row count has not unexpectedly expanded.

    Why this gate exists:
        Pre-aggregation should prevent row explosion from joins.
        But this gate catches any case where it doesn't — e.g., if the events
        aggregation produced more than one row per user due to a bug, or if
        the segments table has duplicate user_ids.

    Args:
        df_input: Deduplicated input orders DataFrame (baseline count).
        df_output: Final enriched output DataFrame.
        threshold: Maximum acceptable ratio of output/input counts.

    Raises:
        ValueError: If output row count exceeds input × threshold.
    """
    input_count  = df_input.count()
    output_count = df_output.count()
    ratio        = output_count / input_count if input_count > 0 else 0

    logger.info(f"Row count quality gate: input={input_count:,}, output={output_count:,}, ratio={ratio:.3f}")

    if ratio > threshold:
        raise ValueError(
            f"ROW EXPLOSION DETECTED — pipeline halted. "
            f"Input: {input_count:,}, Output: {output_count:,}, "
            f"Ratio: {ratio:.3f} > threshold: {threshold}. "
            f"Investigate join logic and input data."
        )

    logger.info(f"Row count quality gate PASSED (ratio {ratio:.3f} ≤ {threshold})")


# ── Output ─────────────────────────────────────────────────────────────────────

def write_enriched_output(df_final: DataFrame, processed_bucket: str) -> None:
    """
    Write the enriched DataFrame to GCS Processed as date-partitioned Parquet.

    Idempotency:
        partitionOverwriteMode=dynamic (set on SparkSession) ensures that only
        the process_date partition(s) present in df_final are overwritten.
        All other historical partitions remain untouched.

    Compression:
        Snappy — splittable, fast decompression, good compression ratio.
        Gzip would be smaller but not splittable (single reader per file).

    Args:
        df_final: Final enriched DataFrame to write.
        processed_bucket: GCS bucket URI for enriched output.
    """
    output_path = f"{processed_bucket}/enriched_orders/"
    logger.info(f"Writing enriched output to: {output_path}")

    (
        df_final.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("process_date")
        .parquet(output_path)
    )

    logger.info(f"Write complete: {output_path}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main(date: str, env: str) -> None:
    """
    Main entry point for the daily enrichment job.

    Args:
        date: Processing date in YYYY-MM-DD format (from Airflow {{ ds }}).
        env: Deployment environment — "prod" or "dev".
    """
    config = CONFIGS[env]
    validated_bucket = config["validated_bucket"]
    processed_bucket = config["processed_bucket"]

    logger.info(f"Starting RetailEdge Daily Enrichment | date={date} | env={env}")

    spark = create_spark_session()

    try:
        # ── Read ───────────────────────────────────────────────────────────────
        df_orders   = read_orders(spark, validated_bucket, date)
        df_events   = read_events(spark, validated_bucket, date)
        df_segments = read_segments(spark, validated_bucket)

        # ── Transform ──────────────────────────────────────────────────────────
        df_orders_deduped = deduplicate_orders(df_orders)
        df_events_agg     = aggregate_events(df_events)
        df_enriched       = enrich_orders(df_orders_deduped, df_events_agg, df_segments)
        df_cleaned        = apply_business_rules(df_enriched)
        df_final          = cast_and_select(df_cleaned)

        # ── Quality Gate ───────────────────────────────────────────────────────
        assert_no_row_explosion(df_orders_deduped, df_final)

        # ── Write ──────────────────────────────────────────────────────────────
        write_enriched_output(df_final, processed_bucket)

        logger.info(f"SUCCESS — Enrichment job complete for date={date}")

    except Exception as exc:
        logger.error(f"FAILURE — Enrichment job failed for date={date}: {exc}", exc_info=True)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RetailEdge Daily Enrichment Job")
    parser.add_argument(
        "--date",
        required=True,
        type=str,
        help="Processing date in YYYY-MM-DD format. Passed by Airflow as {{ ds }}. "
             "NEVER use datetime.today() — breaks on retry.",
    )
    parser.add_argument(
        "--env",
        choices=["prod", "dev"],
        default="prod",
        help="Deployment environment.",
    )
    args = parser.parse_args()

    # Validate date format before starting Spark
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Expected YYYY-MM-DD.")
        sys.exit(1)

    main(date=args.date, env=args.env)
