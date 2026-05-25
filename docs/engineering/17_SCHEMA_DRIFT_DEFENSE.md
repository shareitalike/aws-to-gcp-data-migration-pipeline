# Schema Drift Defense — Three-Layer Strategy

**Document Type:** Engineering Deep Dive  
**Category:** Data Quality  
**Why This Matters:** Schema drift — when the upstream team changes the structure of incoming data without notice — is the #1 cause of silent pipeline failures in production. Our pipeline defends against it at three independent layers.

---

## What Is Schema Drift?

Schema drift occurs when the schema (column names, types, presence) of incoming data changes unexpectedly:

| Type | Example | Risk |
|:---|:---|:---|
| **Column removed** | `order_id` deleted from the source | Pipeline crashes or produces NULLs |
| **Column renamed** | `event_type` → `action_type` | JOIN produces NULLs silently |
| **Type changed** | `amount` changes from DOUBLE to STRING | Aggregations fail or return wrong values |
| **Column added** | New `discount_code` column appears | No risk if handled correctly |
| **Nullable changed** | Required column becomes nullable | Data quality silently degrades |

### The Silent Failure Problem

The most dangerous aspect of schema drift: **pipelines often don't crash — they produce wrong data silently.**

Our production incident (Month 4): The OMS team changed `event_type` → `action_type` in the events file. Our validator only checked `orders` and `user_segments` files — not events. The events file passed validation. Spark joined on `event_type`, got NULLs. Analysts saw blank event dimensions for 2 days before anyone noticed.

---

## Our Three-Layer Defense

```
Layer 1: Cloud Run Validation Gate (DETECTION)
         ↓ PASS or QUARANTINE
Layer 2: PySpark .select() Firewall (ABSORPTION)
         ↓ ONLY approved columns flow through
Layer 3: dbt on_schema_change (CONTROLLED EVOLUTION)
         ↓ New columns added to production only after formal approval
```

---

## Layer 1: Cloud Run Validation Gate (Detection)

### What It Does
Every file is checked against a `EXPECTED_SCHEMAS` registry before any compute is wasted on it.

### The Code

```python
# 02_ingestion/validate_landing.py

EXPECTED_SCHEMAS = {
    "orders": {
        "order_id":     {"bq_type": "STRING",  "required": True},
        "user_id":      {"bq_type": "STRING",  "required": True},
        "amount":       {"bq_type": "DOUBLE",  "required": True},
        "currency":     {"bq_type": "STRING",  "required": True},
        "process_date": {"bq_type": "DATE",    "required": True},
        "channel":      {"bq_type": "STRING",  "required": False},
        "product_sku":  {"bq_type": "STRING",  "required": False},
    },
    "events": {
        "event_id":     {"bq_type": "STRING",  "required": True},
        "user_id":      {"bq_type": "STRING",  "required": True},
        "event_type":   {"bq_type": "STRING",  "required": True},   # ← Added after incident
        "event_time":   {"bq_type": "TIMESTAMP","required": True},
    },
    "user_segments": {
        "user_id":      {"bq_type": "STRING",  "required": True},
        "user_segment": {"bq_type": "STRING",  "required": True},
        "segment_date": {"bq_type": "DATE",    "required": True},
    }
}

def validate_schema(file_path: str, file_type: str) -> tuple[bool, str]:
    """
    Validate incoming Parquet file schema against the registered contract.
    Returns (is_valid: bool, failure_reason: str)
    
    Key principle: We check for MISSING required columns, not for EXTRA columns.
    Extra columns pass validation — they are handled silently by Spark's .select()
    """
    import pyarrow.parquet as pq
    
    schema = pq.read_schema(file_path)
    actual_columns = {field.name: str(field.type) for field in schema}
    expected = EXPECTED_SCHEMAS.get(file_type, {})
    
    for col_name, col_spec in expected.items():
        if col_spec["required"] and col_name not in actual_columns:
            return False, f"Required column '{col_name}' missing from {file_type} file"
    
    return True, ""
```

### What Happens on Failure

```python
def process_file(event: dict, context) -> None:
    file_path = event["name"]
    file_type  = detect_file_type(file_path)   # "orders", "events", "user_segments"
    
    # Check 1: Deduplication
    if is_duplicate(file_path):
        logger.info(f"Duplicate file skipped: {file_path}")
        return
    
    # Check 2: Schema validation
    is_valid, reason = validate_schema(file_path, file_type)
    
    if is_valid:
        # Promote to validated bucket
        copy_to_validated(file_path)
        update_firestore(file_path, status="validated")
    else:
        # Move to quarantine (file is PRESERVED, not deleted)
        copy_to_quarantine(file_path)
        update_firestore(file_path, status="quarantined", reason=reason)
        
        # Alert engineering team immediately
        send_slack_alert(
            channel="#data-ops-alerts",
            message=f":warning: File quarantined: `{file_path}`\nReason: {reason}\nFile type: {file_type}"
        )
```

### Key Design Principle: Check for MISSING, Not for EXTRA

We only block files that are **missing required columns**. We do NOT block files that have **extra columns** (new columns added by upstream). Why?

- Missing required columns break our pipeline downstream
- Extra columns are safely absorbed by Layer 2 (Spark's `.select()`)
- Blocking extra columns would quarantine legitimate file drops unnecessarily

---

## Layer 2: PySpark `.select()` Firewall (Absorption)

### What It Does
At the end of every PySpark transformation, an explicit `.select()` ensures only approved columns flow to the output. Any column not in this list — regardless of what the upstream added — is silently dropped.

### The Code

```python
# 03_processing/process_daily_orders.py

def write_enriched_output(df_enriched: DataFrame, output_path: str) -> None:
    """
    Write final enriched output with ONLY approved columns.
    
    This .select() is the Layer 2 schema drift defense:
    - Any extra columns added upstream are silently dropped here
    - They do NOT propagate to GCS Processed, BigQuery staging, or production
    - They can only flow through after a formal approval + code change
    """
    df_final = df_enriched.select(
        col("order_id").cast("string"),
        col("user_id").cast("string"),
        col("amount").cast("double"),
        col("currency").cast("string"),
        col("process_date").cast("date"),
        col("user_segment").cast("string"),
        col("event_count").cast("integer"),
        # NOTE: When a new column is formally approved, ADD IT HERE
        # Example: col("discount_code").cast("string"),
    )
    
    df_final.write \
        .mode("overwrite") \
        .partitionBy("process_date") \
        .parquet(output_path)
```

### What Happened in Practice

In Month 6, the OMS team added a new column `channel_code` to the orders file (online/in-store/marketplace indicator). The file:
- ✅ Passed the Cloud Run validator (it's an extra column, not missing a required one)
- ✅ Was read by Spark into the DataFrame (Parquet is self-describing)
- ✅ But `channel_code` was NOT in the `.select()` — it was silently dropped
- ✅ GCS Processed Parquet had no `channel_code`
- ✅ BigQuery staging had no `channel_code`
- ✅ Production table was unchanged

The upstream team's release had **zero impact** on our pipeline. No emergency coordination. No downtime. No alert.

When the business later asked for `channel_code` in analytics (2 months later), we:
1. Added `col("channel_code").cast("string")` to the `.select()`
2. The next daily run carried `channel_code` through to GCS Processed
3. Layer 3 (dbt) handled it automatically

---

## Layer 3: dbt `on_schema_change` (Controlled Evolution)

### What It Does
When a new column flows into BigQuery staging (after being approved through Layer 2), dbt automatically adds it to the production table without a full rebuild.

### The dbt Model Config

```sql
-- 05_transformation/models/core/enriched_orders.sql

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            "field": "process_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['user_segment', 'user_id'],
        on_schema_change='append_new_columns'   -- ← THE KEY SETTING
    )
}}

SELECT
    order_id,
    user_id,
    amount,
    currency,
    process_date,
    user_segment,
    event_count,
    CURRENT_TIMESTAMP() AS inserted_at
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
WHERE process_date = '{{ var("execution_date") }}'
{% endif %}
```

### What `on_schema_change='append_new_columns'` Does

When dbt detects that the staging table has a column that the production table doesn't have:
```sql
-- dbt automatically executes this:
ALTER TABLE `retailedge-data-prod.core.enriched_orders`
ADD COLUMN channel_code STRING;

-- Then the MERGE runs and populates channel_code for new/updated rows
```

**No manual DDL. No `--full-refresh` (which would truncate history). No deployment window required.**

### The Formal Approval Process

New column lifecycle:
```
1. OMS team adds column to their file (passes Layer 1 gate, dropped by Layer 2)
2. Business team requests the column in analytics → creates a ticket
3. Data engineer reviews → adds to Spark .select() (Layer 2 approved)
4. Next daily run carries column to GCS Processed → BigQuery staging picks it up
5. Next dbt run: dbt detects new column in staging → ALTER TABLE in production
6. Update data_contract.md with the new column + approval date
```

---

## Breaking Changes: A Different Protocol

Not all schema changes are safely absorbable. **Breaking changes require a different protocol:**

| Breaking Change | Why It's Breaking | Protocol |
|:---|:---|:---|
| Column renamed (`event_type` → `action_type`) | Our Spark JOIN fails silently | COALESCE transition + contract version |
| Column type changed (`amount`: DOUBLE → STRING) | Aggregations fail or silently wrong | Explicit CAST + UAT before promote |
| Column removed | Missing required column → quarantine | Contract version bump + upstream coordination |

### Breaking Change Recovery Example (Our Actual Incident)

**The incident:** `event_type` renamed to `action_type` in events file.

**The fix:**
```python
# Transition-safe Spark code (runs for 30 days during the transition window)
df_events = df_events.withColumn(
    "event_type",
    coalesce(col("event_type"), col("action_type"))  # Accept both old and new column names
)
```

**Data Contract updated:**
```markdown
## Change Log
| Date | Column | Change | Type | Approval |
|:---|:---|:---|:---|:---|
| 2024-07-15 | event_type (events) | Renamed to action_type | BREAKING | Priya Sharma (RetailEdge) |
| Transition: 2024-07-15 → 2024-08-15 | COALESCE fix active | — | — |
| 2024-08-16 | action_type | Transition complete | RESOLVED | — |
```

---

## Interview Answer (Say This Out Loud)

> *"Schema drift — when the upstream team changes their file structure without telling you — is the #1 cause of silent pipeline failures. Pipelines don't crash; they just start producing wrong data. So we built a three-layer defense.*
>
> *Layer 1 is the Cloud Run validation gate. Every incoming file is checked against a schema contract — a registry of required columns and their types for each file type. If any required column is missing, the file is quarantined immediately and a Slack alert fires with the exact column name. We only block missing required columns — extra columns are intentionally allowed through.*
>
> *Layer 2 is the PySpark `.select()` firewall. At the end of our Spark job, an explicit `.select()` lists only the approved output columns. Any extra upstream columns that made it past the validator — say the OMS team added a `channel_code` column — are silently dropped here. They never reach GCS Processed, BigQuery staging, or production. Zero pipeline impact.*
>
> *Layer 3 is dbt's `on_schema_change='append_new_columns'`. When a new column is formally approved and added to the Spark `.select()`, it flows into BigQuery staging. dbt detects the new column in staging, runs an `ALTER TABLE ADD COLUMN` on the production table automatically — no manual DDL, no `--full-refresh`. Controlled, audited evolution.*
>
> *Breaking changes — column renamed, type changed, column removed — are treated as incidents with a formal contract versioning process, COALESCE transition fixes, and coordinated releases."*

---

## Related Documents
- [Data Contract](19_DATA_CONTRACT.md)
- [Quarantine Recovery](20_QUARANTINE_RECOVERY.md)
- [Why GCS Not Direct](../design_decisions/08_WHY_GCS_NOT_DIRECT.md)
