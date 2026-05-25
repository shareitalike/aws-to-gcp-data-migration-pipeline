# Idempotency Design — Every Stage Explained

**Document Type:** Engineering Deep Dive  
**Category:** Reliability & Correctness  
**Why This Matters:** Idempotency is the property that makes retries safe. In production, failures are not exceptional — they're expected. Idempotency means: run it once, run it ten times, get the same result.

---

## What Is Idempotency?

**Definition:** An operation is idempotent if running it multiple times with the same input always produces the same output.

**Why it matters in data pipelines:**
- Cloud infrastructure has transient failures (quota limits, network blips, container crashes)
- Airflow will retry failed tasks — sometimes immediately, sometimes hours later
- Without idempotency: retries create duplicates, corrupt analytics, require manual cleanup
- With idempotency: retries are free — run as many as you need, the result is always correct

**Real-world failure scenarios we protected against:**
1. S3 transfer runs twice (network retry sends the same file twice)
2. Cloud Run validator fires twice for the same file (GCS event delivered twice — at-least-once semantics)
3. Spark job re-runs for the same date (Airflow manual retry)
4. BigQuery load job re-runs (transient quota error → Airflow retry)
5. dbt MERGE runs twice (Airflow retry after brief failure)

---

## Idempotency Map — Our Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│  Stage                │  Idempotency Mechanism                  │
│─────────────────────────────────────────────────────────────────│
│  1. File Transfer     │  Source is immutable (same file = same  │
│                       │  bytes). GCS overwrites same key = safe │
│─────────────────────────────────────────────────────────────────│
│  2. Cloud Run Valid.  │  MD5 hash check in Firestore            │
│                       │  Same file → same hash → SKIP           │
│─────────────────────────────────────────────────────────────────│
│  3. PySpark Job       │  partitionOverwriteMode = dynamic       │
│                       │  Re-run for date X → overwrites only    │
│                       │  process_date=X partition               │
│─────────────────────────────────────────────────────────────────│
│  4. BQ Staging Load   │  WRITE_TRUNCATE                         │
│                       │  Full wipe + reload every run           │
│─────────────────────────────────────────────────────────────────│
│  5. dbt MERGE         │  MERGE ON order_id                      │
│                       │  Matched rows: UPDATE to same values    │
│                       │  Unmatched rows: INSERT once            │
│─────────────────────────────────────────────────────────────────│
│  6. Airflow Date      │  {{ ds }} logical date (not datetime.   │
│                       │  today()) — retry always uses original  │
│                       │  scheduled date                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Stage 1: File Transfer (S3 → GCS)

### Mechanism: GCS Object Idempotency

GCS uses an object key model. Writing the same object key multiple times simply overwrites with the same bytes:
```
gs://retailedge-raw-prod/orders/orders_2025-06-01.parquet
```
If the `S3ToGCSOperator` runs twice (Airflow retry), it writes the same bytes to the same GCS key. The second write produces exactly the same file — no duplicate, no corruption.

**No special code needed** — GCS object semantics are naturally idempotent for overwrites.

---

## Stage 2: Cloud Run File Validation

### Mechanism: MD5 Hash Deduplication via Firestore

This is the most important idempotency mechanism in the pipeline, because GCS Event Triggers use **at-least-once delivery** — the same event CAN be delivered twice.

```python
def is_duplicate(file_path: str) -> bool:
    """
    Compute MD5 hash of the file and check Firestore.
    Returns True if this exact file has already been processed.
    
    Why MD5 and not filename?
    - The OMS team might re-drop the SAME filename with DIFFERENT content (bug fix)
    - A different MD5 = different content = treat as new file
    - The same MD5 = same content = duplicate = skip
    """
    from google.cloud import storage, firestore
    import hashlib
    
    storage_client = storage.Client()
    blob = storage_client.bucket(BUCKET_NAME).blob(file_path)
    
    # Download and hash the file content
    content = blob.download_as_bytes()
    file_md5 = hashlib.md5(content).hexdigest()
    
    db = firestore.Client()
    doc_ref = db.collection("file_hashes").document(file_md5)
    doc = doc_ref.get()
    
    if doc.exists:
        logger.info(f"Duplicate detected. Hash {file_md5} already processed.")
        return True
    
    # Atomic write: mark as processing BEFORE doing any work
    # Prevents race conditions if two Cloud Run instances process the same file simultaneously
    doc_ref.set({
        "filename": file_path,
        "status": "processing",
        "first_seen_at": firestore.SERVER_TIMESTAMP
    })
    return False
```

**Why Firestore and not a local dict or SQLite?**

Cloud Run is stateless. Multiple instances can run simultaneously. A local dict or SQLite file lives only in one container's memory/disk — invisible to other instances. Firestore is a globally distributed database — all instances share the same state. Atomic writes prevent race conditions.

---

## Stage 3: PySpark Enrichment

### Mechanism: `partitionOverwriteMode = dynamic`

This Spark configuration is the key to making Spark re-runs safe for specific dates:

```python
# 03_processing/process_daily_orders.py

spark = SparkSession.builder \
    .appName("RetailEdge-DailyEnrichment") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# Write output partitioned by process_date
df_final.write \
    .mode("overwrite") \
    .partitionBy("process_date") \
    .parquet("gs://retailedge-processed-prod/enriched_orders/")
```

**What `partitionOverwriteMode = dynamic` does:**

| Mode | Behavior |
|:---|:---|
| Default (static) | `mode("overwrite")` deletes ALL partitions and rewrites from scratch |
| `dynamic` | `mode("overwrite")` deletes ONLY the partitions present in the DataFrame being written |

**Example:**
- DataFrame contains only `process_date = 2025-06-01` rows
- With `static`: deletes ALL partitions (wipes 2 years of history!) then writes June 1st
- With `dynamic`: deletes ONLY the `process_date=2025-06-01` partition, rewrites it. June 2nd, June 3rd... untouched.

**This is how we safely replay any single day:**
```bash
# Airflow: clear and retry the spark task for a specific DAG run dated 2025-06-01
# Spark job writes only to process_date=2025-06-01
# Every other date's data: completely untouched
```

---

## Stage 4: BigQuery Staging Load

### Mechanism: `WRITE_TRUNCATE`

```python
# 04_warehouse/load_staging.py

from google.cloud import bigquery

load_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Wipe + reload
    autodetect=True,
)

client.load_table_from_uri(
    source_uris=f"gs://retailedge-processed-prod/enriched_orders/process_date={execution_date}/*.parquet",
    destination=f"{PROJECT_ID}.staging.orders_daily",
    job_config=load_config,
).result()
```

**What `WRITE_TRUNCATE` means:**
- Before loading: DELETE all rows from `staging.orders_daily`
- Then: INSERT all rows from the GCS Parquet files

Running this 3 times for the same date produces exactly the same staging table every time. No accumulation of duplicates. No half-loaded states.

**Why staging is safe to truncate:** Staging is ephemeral and exists only to feed the MERGE. It is never directly queried by analysts. `WRITE_TRUNCATE` on staging is the right approach.

**Why production is NOT truncated:** The production table (`core.enriched_orders`) uses MERGE — not WRITE_TRUNCATE — because it accumulates history across dates. Truncating it would wipe 2 years of analytics.

---

## Stage 5: dbt MERGE (Production Table)

### Mechanism: `MERGE` on `order_id`

```sql
-- Compiled by dbt for enriched_orders incremental model
MERGE INTO `retailedge-data-prod.core.enriched_orders` AS target
USING (
    SELECT * FROM `retailedge-data-prod.staging.orders_daily`
    WHERE process_date = '2025-06-01'
) AS source
ON target.order_id = source.order_id
AND target.process_date = source.process_date

WHEN MATCHED THEN UPDATE SET
    target.amount       = source.amount,
    target.user_segment = source.user_segment,
    target.event_count  = source.event_count,
    target.updated_at   = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    order_id, user_id, amount, currency, process_date,
    user_segment, event_count, inserted_at
) VALUES (
    source.order_id, source.user_id, source.amount, source.currency,
    source.process_date, source.user_segment, source.event_count,
    CURRENT_TIMESTAMP()
);
```

**Why MERGE instead of INSERT:**

| Operation | What happens on second run | Safe? |
|:---|:---|:---|
| `INSERT` | Adds duplicate rows | ❌ NO |
| `INSERT OR IGNORE` | Skips existing rows (GCP doesn't support this) | — |
| `WRITE_TRUNCATE` + INSERT | Deletes all history, reloads only today | ❌ NO (loses history) |
| `MERGE` | Updates existing rows, inserts new ones | ✅ YES |

**Running the same MERGE 10 times:**
- Run 1: Inserts `ORD-2025-061-87432` (not found → INSERT)
- Run 2: Finds `ORD-2025-061-87432` → UPDATE to same values (no change in data)
- Run 3–10: Same as run 2

The production table state after run 10 = production table state after run 1. **That is idempotency.**

---

## Stage 6: Airflow `{{ ds }}` — The Logical Date

### Mechanism: Template Variable Instead of Wall-Clock Time

```python
# 06_orchestration/dags/retailedge_daily_pipeline.py

# ❌ WRONG — breaks on retry
from datetime import datetime
execution_date = datetime.today().strftime("%Y-%m-%d")
# If March 19th's pipeline fails and retries on March 22nd:
# datetime.today() → "2026-03-22"
# Spark writes to process_date=2026-03-22 (WRONG!)
# March 19th is never processed

# ✅ CORRECT — always correct on retry
DataprocSubmitJobOperator(
    task_id="trigger_dataproc_spark",
    job={
        "pyspark_job": {
            "args": ["--date", "{{ ds }}"],  # Always the logical execution date
        }
    }
)
# If March 19th's pipeline fails and retries on March 22nd:
# {{ ds }} → "2026-03-19" (correct)
# Spark writes to process_date=2026-03-19 (correct!)
```

**This is what makes Airflow backfills work:**
```bash
# Replay 7 days of history
airflow dags backfill \
  --start-date 2025-06-01 \
  --end-date 2025-06-07 \
  retailedge_daily_pipeline

# Each DAG run uses {{ ds }} = its own scheduled date
# June 1st run processes June 1st data
# June 2nd run processes June 2nd data
# etc.
```

---

## The Retry Scenario Walkthrough

**Scenario:** The `load_bq_staging` task fails at 00:31 due to a transient BigQuery quota error. Airflow retries it automatically.

| Task | Run 1 Status | Re-run Status | Why Safe |
|:---|:---|:---|:---|
| `transfer_s3_to_gcs` | ✅ SUCCESS | Not re-run | Airflow preserves state |
| `check_file_validation` | ✅ SUCCESS | Not re-run | Airflow preserves state |
| `trigger_dataproc_spark` | ✅ SUCCESS | Not re-run | Airflow preserves state |
| `load_bq_staging` | ❌ FAILED | ✅ Re-run | WRITE_TRUNCATE → same result |
| `run_dbt_incremental` | ⏸ SKIPPED | ✅ Re-run | MERGE → idempotent |

**Result:** Identical production table state after retry vs. after a clean first run.

---

## Interview Answer (Say This Out Loud)

> *"Idempotency means that running a pipeline multiple times with the same input always produces the same output. It's essential because in production, failures are not exceptional — they're expected. If your pipeline can't be safely retried, every failure becomes a data emergency requiring manual cleanup.*
>
> *We achieved idempotency at every stage. At the file ingestion stage: Cloud Run computes an MD5 hash of every incoming file and checks Firestore. If the hash already exists — meaning we've seen this exact file before — the file is skipped entirely. This handles GCS event trigger at-least-once delivery and duplicate S3 drops.*
>
> *At the Spark stage: we configured `partitionOverwriteMode = dynamic`. When Spark writes a date's data, it overwrites only that date's partition — it never touches other partitions. Re-running the Spark job for June 1st overwrites only June 1st's data; June 2nd, June 3rd are completely untouched.*
>
> *At the BigQuery staging load: we use `WRITE_TRUNCATE` — the staging table is wiped and reloaded completely every run. Running the load three times produces the same staging table as running it once.*
>
> *At the production table: we use a MERGE statement on `order_id`. If a row already exists, it's updated to the same values. If it's new, it's inserted. Running the same MERGE ten times in a row produces identical results — that's idempotency at the database level.*
>
> *And critically: all date parameters use Airflow's `{{ ds }}` logical execution date — never `datetime.today()`. This means a retry on March 22nd for March 19th's pipeline still processes March 19th's data. Without this, retries would produce phantom data for the wrong date."*

---

## Related Documents
- [Why Cloud Composer](../design_decisions/11_WHY_COMPOSER.md)
- [Spark Join Strategy](21_SPARK_JOIN_STRATEGY.md)
- [Data Flow](../architecture/06_DATA_FLOW.md)
