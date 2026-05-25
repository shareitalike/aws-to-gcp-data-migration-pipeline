# Data Flow — End-to-End Record Lifecycle

**Document Type:** Engineering Reference  
**Purpose:** Trace a single order record from source to analytics dashboard

> **How to use this in an interview:**  
> When asked *"Walk me through your pipeline end-to-end"*, use this document as your mental map.  
> Pick a concrete record (order ID: ORD-2025-061-87432) and trace it through every layer.  
> This makes your answer specific and demonstrates real engineering depth.

---

## The Record We're Tracing

**Order placed at 14:23 IST on June 1, 2025 by a RetailEdge customer:**

```
order_id:     ORD-2025-061-87432
user_id:      USR-9847231
amount:       ₹2,450.00
currency:     INR
channel:      online
product_sku:  SKU-ELEC-4821
process_date: 2025-06-01
```

---

## Step 1 — Source System (OMS, 23:30 IST)

The RetailEdge Order Management System runs its nightly batch job. It extracts all orders placed on June 1, 2025 and writes them to AWS S3.

**File created:**
```
s3://retailedge-oms-export/orders/orders_2025-06-01.parquet
```

**File metadata:**
- Format: Parquet (Snappy compressed)
- Size: ~148MB
- Row count: 98,214 orders
- Schema embedded in Parquet footer: order_id (STRING), user_id (STRING), amount (DOUBLE), currency (STRING), channel (STRING), product_sku (STRING), process_date (DATE)

Our record (ORD-2025-061-87432) is **row 47,293** in this file.

---

## Step 2 — Cross-Cloud Transfer (Airflow, 00:00 IST)

The Cloud Composer Airflow DAG (`retailedge_daily_pipeline`) is scheduled for midnight IST.

**Task: `transfer_s3_to_gcs`**
- Operator: `S3ToGCSOperator`
- Source: `s3://retailedge-oms-export/orders/orders_2025-06-01.parquet`
- Destination: `gs://retailedge-raw-prod/orders/orders_2025-06-01.parquet`
- Authentication: AWS credentials from GCP Secret Manager

**What happens to our record:**  
The entire Parquet file — including our record — is transferred byte-for-byte to GCS. No transformation. No parsing. The file in GCS is an exact binary copy of the S3 file. Duration: ~90 seconds for 148MB.

---

## Step 3 — Event-Driven Validation (Cloud Run, 00:01:30 IST)

The moment the file lands in `gs://retailedge-raw-prod/`, a GCS Event Trigger fires the Cloud Run validator container. This happens within milliseconds of the file write completing — it does NOT wait for Airflow.

**Validation Check 1 — Deduplication (MD5 Hash):**
```python
file_md5 = compute_md5("gs://retailedge-raw-prod/orders/orders_2025-06-01.parquet")
# Result: "a3f8e2d9c1b745f0e8d3a2c1b9f8e7d6"

existing = firestore_client.collection("file_hashes").document(file_md5).get()
# Result: Does not exist (this is a new file)

# Action: Write hash to Firestore with status='processing'
firestore_client.collection("file_hashes").document(file_md5).set({
    "filename": "orders_2025-06-01.parquet",
    "file_type": "orders",
    "status": "processing",
    "first_seen_at": "2025-06-02T18:31:42Z"
})
```

**Validation Check 2 — Schema Validation:**
```python
EXPECTED_SCHEMAS["orders"] = {
    "order_id":     {"type": "STRING",  "required": True},
    "user_id":      {"type": "STRING",  "required": True},
    "amount":       {"type": "DOUBLE",  "required": True},
    "currency":     {"type": "STRING",  "required": True},
    "channel":      {"type": "STRING",  "required": False},
    "product_sku":  {"type": "STRING",  "required": False},
    "process_date": {"type": "DATE",    "required": True},
}

# PyArrow reads only the Parquet footer (schema metadata) — does NOT read the data rows
schema = pq.read_schema("gs://retailedge-raw-prod/orders/orders_2025-06-01.parquet")
# Validates all required columns present and types match

# Result: PASS ✅
```

**Validation Result: PASS**  
File is copied to `gs://retailedge-validated-prod/orders/orders_2025-06-01.parquet`

**What happens to our record:**  
It moves from `raw` to `validated` as part of the file. Still untouched, still Parquet.

---

## Step 4 — PySpark Enrichment (Serverless Dataproc, ~00:10 IST)

Airflow's `trigger_dataproc_spark` task triggers a Serverless Dataproc PySpark job.

**Cluster spin-up:** ~90 seconds (serverless — no persistent cluster)

**PySpark Operations on our record:**

### 4a — Read all three validated files
```python
df_orders   = spark.read.parquet("gs://retailedge-validated-prod/orders/orders_2025-06-01.parquet")
df_events   = spark.read.parquet("gs://retailedge-validated-prod/events/events_2025-06-01.parquet")
df_segments = spark.read.parquet("gs://retailedge-validated-prod/segments/user_segments_20250601.parquet")
```

### 4b — Aggregate events (prevent row explosion)
```python
# User USR-9847231 has 23 events on June 1 (page views, add-to-cart, checkout)
# Naive join would create 23 copies of ORD-2025-061-87432
# Pre-aggregation collapses this to 1 row per user

df_events_agg = df_events.groupBy("user_id").agg(
    count("event_type").alias("event_count"),           # USR-9847231: 23
    collect_set("event_type").alias("event_types")      # ['view', 'cart', 'checkout', 'purchase']
)
# df_events_agg now has 1 row per user, not 23 rows for USR-9847231
```

### 4c — Broadcast join with segments
```python
# user_segments is 200MB — small enough to broadcast
# No shuffle join needed — each executor has a local copy of the full segments table
df_orders_enriched = df_orders.join(
    broadcast(df_segments), on="user_id", how="left"
).join(
    df_events_agg, on="user_id", how="left"
)
# USR-9847231 is in segment "premium_buyer"
# ORD-2025-061-87432 gets: user_segment="premium_buyer", event_count=23
```

### 4d — Deduplication
```python
window = Window.partitionBy("order_id").orderBy(col("process_date").desc())
df_deduped = df_orders_enriched.withColumn("rn", row_number().over(window)) \
                               .filter(col("rn") == 1).drop("rn")
# ORD-2025-061-87432 appears once — passes dedup cleanly
```

### 4e — Business rules
```python
df_filtered = df_deduped.filter(col("amount") > 0)
# ₹2,450 > 0 — passes ✅
```

### 4f — Type casting (schema normalization)
```python
df_final = df_filtered.select(
    col("order_id").cast("string"),
    col("user_id").cast("string"),
    col("amount").cast("double"),
    col("currency").cast("string"),
    col("process_date").cast("date"),
    col("user_segment").cast("string"),
    col("event_count").cast("integer")
    # Any extra upstream columns are NOT in this .select() — silently dropped
)
```

### 4g — Row count quality gate
```python
input_count  = df_orders_deduped.count()   # 98,214
output_count = df_final.count()            # 98,014 (some zero-amount orders filtered)
assert output_count <= input_count * 1.1   # 98,014 ≤ 108,035 ✅
```

### 4h — Write to GCS Processed
```python
df_final.write \
    .mode("overwrite") \
    .partitionBy("process_date") \
    .parquet("gs://retailedge-processed-prod/enriched_orders/")
```

**Output path:** `gs://retailedge-processed-prod/enriched_orders/process_date=2025-06-01/part-00003.snappy.parquet`

**Our record (row 47,293 of input) is now:**
```
order_id:     ORD-2025-061-87432
user_id:      USR-9847231
amount:       2450.0
currency:     INR
process_date: 2025-06-01
user_segment: premium_buyer
event_count:  23
```

---

## Step 5 — BigQuery Staging Load (~00:30 IST)

**Task: `load_bq_staging`**

```python
# WRITE_TRUNCATE: staging table is wiped and reloaded completely
# autodetect=True reads schema from our normalized Parquet (not raw upstream)
# Our record is now in staging.orders_daily alongside 98,013 other records
```

**Staging table state:**
```sql
SELECT * FROM `retailedge-data-prod.staging.orders_daily` 
WHERE order_id = 'ORD-2025-061-87432';
-- Returns 1 row, exactly as Spark produced it
```

---

## Step 6 — dbt Incremental Merge (~00:35 IST)

**Task: `run_dbt_incremental`**

dbt compiles the `enriched_orders` incremental model and executes a MERGE in BigQuery:

```sql
MERGE INTO `retailedge-data-prod.core.enriched_orders` AS target
USING (
    SELECT * FROM `retailedge-data-prod.staging.orders_daily`
    WHERE process_date = '2025-06-01'   -- partition pruning
) AS source
ON target.order_id = source.order_id
AND target.process_date = source.process_date

WHEN MATCHED THEN UPDATE SET
    target.amount        = source.amount,
    target.user_segment  = source.user_segment,
    target.event_count   = source.event_count,
    target.updated_at    = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    order_id, user_id, amount, currency, process_date, user_segment, event_count, inserted_at
) VALUES (
    source.order_id, source.user_id, source.amount, source.currency,
    source.process_date, source.user_segment, source.event_count, CURRENT_TIMESTAMP()
);
```

**For ORD-2025-061-87432:**  
This is a new order — no existing row in `core.enriched_orders`. `WHEN NOT MATCHED` fires. The record is **inserted** into the `process_date=2025-06-01` partition.

**dbt schema tests fire after the model run:**
```yaml
- not_null: [order_id, user_id, amount, process_date]   ✅
- unique: [order_id]                                     ✅
- accepted_values: currency in ['INR', 'USD', 'EUR']    ✅
```

All tests pass. DAG marks `run_dbt_incremental` as SUCCESS.

---

## Step 7 — Available in Looker Studio (~00:42 IST)

**Total pipeline duration: ~42 minutes from midnight to data availability**

Rajesh Mehta (VP Analytics) opens the **Daily Revenue Dashboard** at 09:00 IST. The dashboard queries:
```sql
SELECT 
    user_segment,
    SUM(amount) AS total_revenue,
    COUNT(DISTINCT order_id) AS order_count
FROM `retailedge-data-prod.core.enriched_orders`
WHERE process_date = '2025-06-01'        -- partition filter (required)
GROUP BY user_segment
ORDER BY total_revenue DESC;
```

**Our record contributes:**
- `user_segment = 'premium_buyer'`
- `total_revenue` increased by ₹2,450
- `order_count` increased by 1

**BigQuery query runtime: 2.3 seconds** (was 45 minutes without partitioning)

---

## Idempotency Check — What If This DAG Run Is Retried?

Scenario: The `load_bq_staging` task fails at 00:31 (transient quota error). Airflow retries it at 00:36.

| Stage | What Happens on Retry | Why It's Safe |
|:---|:---|:---|
| `transfer_s3_to_gcs` | Already SUCCESS — not re-run | Airflow preserves task state |
| Cloud Run Validator | Already ran, hash is in Firestore — file skipped | MD5 dedup |
| PySpark | Already SUCCESS — not re-run | Airflow preserves task state |
| `load_bq_staging` | WRITE_TRUNCATE — wipes and reloads staging | Staging is ephemeral by design |
| `run_dbt_incremental` | MERGE — ORD-2025-061-87432 already exists, UPDATE to same values | MERGE idempotency |

**Result: Identical production table regardless of how many retries occurred.**
