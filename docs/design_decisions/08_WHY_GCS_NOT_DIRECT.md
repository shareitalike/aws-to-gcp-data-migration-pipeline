# Design Decision: Why GCS Intermediate Layer Instead of Direct S3 → BigQuery

**Category:** Architecture  
**Decision Date:** February 2024  
**Decision Owner:** Lead Data Engineer (Vipra Soft)  
**Status:** Approved & Implemented

---

## The Question

*"Why did you use GCS as an intermediate landing zone? Why not load directly from S3 into BigQuery? BigQuery supports S3 external tables — you could have just queried S3 data directly."*

---

## What We Chose

**A multi-zone GCS architecture:**
- `GCS Raw` → exact copy of S3 files (Bronze)
- `GCS Validated` → schema-verified, deduplicated files
- `GCS Quarantine` → failed files (preserved for recovery)
- `GCS Processed` → enriched Parquet output from Spark (Silver)

All intermediate processing happens within GCP's internal network.

---

## Alternatives We Evaluated

### Alternative 1: BigQuery External Tables on S3
BigQuery supports `EXTERNAL_TABLE` on S3 via Omni or cross-cloud connectors. You query S3 data directly as if it were a BigQuery table.

**Why we rejected it:**
- **No validation layer**: Any corrupt, schema-broken, or duplicate file is immediately queryable — bad data reaches analysts with zero protection
- **Egress cost every query**: Every query scans S3 data across the network — S3 egress is billed per GB. Running Spark AND analytical queries both hit S3 = paying egress twice
- **Latency**: Cross-cloud query latency (S3 → BigQuery Omni) is higher than native GCS → BigQuery
- **No replay safety**: If you find a bug in your transformation, you're querying live S3 data — you can't "fix" the source and replay

### Alternative 2: Direct S3 → BigQuery Load (No GCS)
Use BigQuery's native S3 data transfer to load files directly without going through GCS.

**Why we rejected it:**
- **No validation control**: Files go straight into BigQuery staging with no schema check or deduplication
- **No Spark integration**: Dataproc jobs read from GCS natively — if the files are only in S3, Spark would pay S3 egress for every run
- **Double egress cost**: Transfer once for BigQuery load + read again for Spark = same file pays egress twice

### Alternative 3: Cloud Run Pulls from S3 Directly
Trigger Cloud Run on a schedule, pull files from S3, validate, then write to BigQuery.

**Why we rejected it:**
- **No immutable history**: If the Cloud Run job transforms the file before storing it, we've lost the original raw data
- **Harder replay**: To replay a day, you'd have to re-pull from S3 (which requires S3 to still have the file) rather than replaying from our own GCS Raw store

---

## Why GCS Won — Three Concrete Reasons

### Reason 1: Data Immutability and Replay Safety

> *"If we loaded directly from S3 to BigQuery and later discovered a bug in our Spark enrichment logic — say a dedup rule that was wrong — we'd need to ask the AWS team to re-expose or re-export the original data, which is operationally painful and sometimes impossible. By landing everything in GCS Raw first, we have our own immutable copy of exactly what arrived. We can replay the entire pipeline from scratch for any historical date without touching AWS at all."*

This happened in practice. In Month 4 of the project, we found a Spark bug that was silently producing wrong `event_count` aggregations. Because we had GCS Raw with the original files intact, we:
1. Fixed the Spark job
2. Replayed 14 days of processing from `gs://retailedge-raw-prod/` without any coordination with the AWS/OMS team
3. Total recovery time: 2 hours (automated)

**Without GCS Raw**, recovery would have required the OMS team to re-export 14 days of historical S3 files — an estimated 3–5 business days of coordination.

### Reason 2: Validation Control

> *"Direct loading from S3 to BigQuery skips the validation gate entirely. Bad files — schema-broken, missing primary key columns, duplicate file drops — would flow straight into your warehouse. By routing through GCS with a Cloud Run validation layer in between, we ensured that only schema-correct, integrity-verified, deduplicated data ever reached BigQuery."*

In the first 3 months of production:
- **14 files quarantined** due to schema violations (upstream OMS team had 3 undocumented schema changes)
- **7 files quarantined** due to duplicate drops (OMS retry behavior)
- **0 corrupt records** ever reached BigQuery production

Without GCS intermediate, all 21 of those files would have silently loaded.

### Reason 3: Network Cost Efficiency

> *"S3 cross-cloud egress is billed per GB. If we loaded directly from S3 to BigQuery AND ran Spark to read from S3 for enrichment, we'd pay twice for the same data leaving AWS. By transferring to GCS once, all subsequent operations — Spark reads, BigQuery loads, validation checks — happen within GCP's internal network at essentially no egress cost."*

**Cost calculation for the RetailEdge project:**
- Daily data volume: ~1.1GB (orders + events + segments combined)
- Monthly: ~33GB
- S3 cross-cloud egress: ~$0.09/GB = **~$3/month** if we ran twice per day ops
- GCS internal traffic cost: **$0** (same-region, same-project traffic is free)

For a single pipeline it seems small. But the client planned to add 6 more pipelines — the cost discipline compounds.

---

## Trade-offs of Our Chosen Approach

| Trade-off | Impact | Our Mitigation |
|:---|:---|:---|
| **Storage cost of GCS Raw** | Storing raw files costs money | Lifecycle policy: delete raw files after 90 days |
| **Added latency (~5 min for transfer)** | Transfer step adds pipeline time | SLA is 6 hours (00:00–06:00 IST) — 5 min is negligible |
| **More complexity (more buckets, more IAM)** | More to manage and monitor | Terraform manages all of it; no manual ops |

---

## Interview Answer (Say This Out Loud)

> *"We evaluated the direct-load option, and there are three concrete reasons we rejected it.*
>
> *First — Data Immutability. If we loaded directly from S3 to BigQuery and later discovered a bug in our enrichment logic — say a dedup rule that was wrong — we'd need to ask the AWS team to re-expose or re-export the original data, which is operationally painful. By landing everything in GCS Raw first, we have our own immutable copy. We can replay the entire pipeline for any historical date without touching AWS. This actually happened in Month 4 — we found a Spark bug, replayed 14 days from GCS Raw in 2 hours, zero coordination needed with the upstream team.*
>
> *Second — Validation Control. Direct loading skips the validation gate. Bad files — schema-broken, duplicate drops — would flow straight into BigQuery. By routing through GCS with a Cloud Run validator, we ensured only schema-correct, deduplicated data ever reached BigQuery. In the first 3 months, we quarantined 21 files that would have silently corrupted the warehouse.*
>
> *Third — Network Cost. S3 cross-cloud egress is billed per GB. If we ran Spark on S3 data AND loaded to BigQuery from S3, we'd pay egress twice for the same data. Transfer once to GCS — all subsequent operations happen within GCP's internal network at essentially no additional cost."*

---

## Related Documents
- [Why Parquet](09_WHY_PARQUET.md)
- [System Architecture](../architecture/05_SYSTEM_ARCHITECTURE.md)
- [Data Flow](../architecture/06_DATA_FLOW.md)
- [Schema Drift Defense](../engineering/17_SCHEMA_DRIFT_DEFENSE.md)
