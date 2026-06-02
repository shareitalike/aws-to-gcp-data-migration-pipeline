# System Architecture — Cloud Data Migration Platform

**Document Type:** Architecture Reference  
**Client:** RetailEdge Global Pvt Ltd  
**Prepared By:** Cloud Data Architecture Group

---

## 1. Architecture Overview

The platform follows a **Medallion Architecture** (Bronze → Silver → Gold) with an event-driven validation gate between landing and processing.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           AWS ECOSYSTEM (Source)                                │
│                                                                                 │
│   [OMS Application]                                                             │
│         │ Daily exports (23:30 IST)                                             │
│         ▼                                                                       │
│   [AWS S3 Bucket]                                                               │
│   ├── orders/orders_YYYY-MM-DD.parquet         (~150MB, ~100K rows)             │
│   ├── events/events_YYYY-MM-DD.parquet         (~800MB, ~2M rows)               │
│   └── segments/user_segments_YYYYMMDD.parquet  (~200MB, ~12M rows, weekly)      │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                    [Cross-Cloud Transfer]
                    Google Storage Transfer Service (STS)
                    Triggered: Scheduled Daily or Ad-Hoc Historical
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER — Raw Landing (GCS)                            │
│                                                                                 │
│   gs://retailedge-raw-prod/                                                     │
│   ├── orders/orders_2025-06-01.parquet                                          │
│   ├── events/events_2025-06-01.parquet                                          │
│   └── segments/user_segments_20250601.parquet                                   │
│                                                                                 │
│                    │ (GCS Event Trigger — fires in milliseconds)                │
│                    ▼                                                            │
│   ┌──────────────────────────────────────────────────────┐                      │
│   │           CLOUD RUN VALIDATOR                        │                      │
│   │                                                      │                      │
│   │  1. Hash Check: MD5 → Firestore (dedup guard)        │                      │
│   │  2. Schema Validation: PyArrow → EXPECTED_SCHEMAS    │                      │
│   │  3. Decision:                                        │                      │
│   │     PASS → gs://retailedge-validated-prod/           │                      │
│   │     FAIL → gs://retailedge-quarantine-prod/          │                      │
│   │             + Slack Alert (#data-ops-alerts)         │                      │
│   └──────────────────────────────────────────────────────┘                      │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                    [DataprocSubmitJobOperator]
                    PySpark on Serverless Dataproc
                    Triggered by Airflow after validation complete
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER — Processed (GCS)                              │
│                                                                                 │
│   PySpark Job Operations:                                                       │
│   ├── Read: gs://retailedge-validated-prod/orders/, events/, segments/          │
│   ├── Aggregate events by user_id (prevent row explosion)                       │
│   ├── Broadcast join: orders + agg_events + segments (on user_id)               │
│   ├── Dedup: ROW_NUMBER() on order_id (keep latest)                             │
│   ├── Business rules: filter zero-amount, cast types                            │
│   ├── Quality gate: assert output_count ≤ input_count × 1.1                     │
│   └── Write: gs://retailedge-processed-prod/enriched_orders/process_date=*/     │
│              (Parquet, Snappy, date-partitioned, partitionOverwriteMode=dynamic)│
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                    [BigQueryInsertJobOperator]
                    WRITE_TRUNCATE to staging
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER — BigQuery Warehouse                            │
│                                                                                 │
│   ┌─────────────────────────────────────────────────┐                           │
│   │  staging.orders_daily  (ephemeral, WRITE_TRUNCATE) │                        │
│   └──────────────────────┬──────────────────────────┘                           │
│                          │                                                      │
│          [KubernetesPodOperator — dbt run]                                      │
│                          │                                                      │
│          MERGE ON order_id (upsert semantics)                                   │
│          on_schema_change='append_new_columns'                                  │
│                          ▼                                                      │
│   ┌─────────────────────────────────────────────────────────────────────────┐   │
│   │  core.enriched_orders                                                   │   │
│   │  PARTITION BY process_date                                              │   │
│   │  CLUSTER BY user_id, user_segment                                       │   │
│   │  require_partition_filter = TRUE                                        │   │
│   └─────────────────────────────────────────────────────────────────────────┘   │
│                          │                                                      │
│          [dbt schema tests — circuit breaker]                                   │
│          ├── not_null: order_id                                                 │
│          ├── unique: order_id                                                   │
│          └── accepted_values: currency in ['INR', 'USD']                        │
└───────────────────────────────┬─────────────────────────────────────────────────┘
                                │
                                ▼
         ┌──────────────────────────────────────┐
         │     LOOKER STUDIO DASHBOARDS         │
         │  - Daily Revenue Report              │
         │  - SKU Movement Analytics            │
         │  - Customer Segment Analysis         │
         │  - Pipeline Health Monitor           │
         └──────────────────────────────────────┘
```

---

## 2. Control Plane — Cloud Composer (Airflow) DAG

The entire pipeline is orchestrated by a single Airflow DAG: `retailedge_daily_pipeline`

```
DAG Schedule: 00:00 IST daily
DAG ID: retailedge_daily_pipeline

┌─────────────────────────────────────────────────────────────┐
│  START                                                       │
│    │                                                         │
│    ▼                                                         │
│  [transfer_s3_to_gcs]          ← S3ToGCSOperator            │
│  Retries: 3 | Backoff: 5 min                                │
│    │                                                         │
│    ▼                                                         │
│  [check_file_validation]       ← GCSSensor (wait for        │
│  Timeout: 60 min                  validated/ files)          │
│    │                                                         │
│    ├── No files → [send_slack_no_data] → END                │
│    │                                                         │
│    ▼                                                         │
│  [trigger_dataproc_spark]      ← DataprocSubmitJobOperator  │
│  Retries: 2 | Timeout: 60 min                               │
│  SLA: 30 min                                                │
│    │                                                         │
│    ▼                                                         │
│  [load_bq_staging]             ← BigQueryInsertJobOperator  │
│  Retries: 3 | WRITE_TRUNCATE                                │
│    │                                                         │
│    ▼                                                         │
│  [run_dbt_incremental]         ← KubernetesPodOperator      │
│  Retries: 1 | dbt run + dbt test                            │
│    │                                                         │
│    ├── dbt test FAIL → [slack_dbt_failure] → FAIL           │
│    │                                                         │
│    ▼                                                         │
│  [pipeline_success_notification]                            │
│  END                                                         │
└─────────────────────────────────────────────────────────────┘

on_failure_callback → Slack alert for ANY task failure
SLA miss callback → Slack alert if total DAG > 4 hours
```

---

## 3. Infrastructure Architecture (Terraform-Managed)

```
GCP Project: retailedge-data-prod
Region: asia-south1 (Mumbai)

GCS Buckets:
├── retailedge-raw-prod           (Bronze landing)
├── retailedge-validated-prod     (Post-validation)
├── retailedge-quarantine-prod    (Failed files)
├── retailedge-processed-prod     (Spark output)
└── retailedge-tf-state           (Terraform remote state)

BigQuery Datasets:
├── staging                       (Ephemeral daily tables)
└── core                          (Production analytics tables)

Firestore Database:
└── file_ingestion_registry       (Deduplication hash store)

Cloud Run Services:
└── file-validator                (Event-driven validation)

IAM Service Accounts:
├── transfer-sa        (roles/storage.objectCreator on raw bucket)
├── validator-sa       (roles/storage.objectAdmin on validated/quarantine, Firestore User)
├── dataproc-sa        (roles/storage.objectViewer on validated, objectCreator on processed)
├── bq-loader-sa       (roles/bigquery.dataEditor on staging dataset)
└── dbt-sa             (roles/bigquery.dataEditor on core dataset, jobUser)
```

---

## 4. Security Architecture

```
Secret Manager:
├── aws-access-key-id             (Used by: Cloud Composer)
├── aws-secret-access-key         (Used by: Cloud Composer)
├── slack-webhook-url             (Used by: Cloud Composer callbacks)
└── dbt-service-account-key       (Used by: dbt KubernetesPod)

Workload Identity Federation:
└── CI/CD pipeline (GitHub Actions) accesses GCP via WIF
    — No long-lived service account keys in CI/CD

VPC Service Controls:
└── BigQuery and GCS restricted to VPC perimeter
    — No public internet access to warehouse data
```

---

## 5. Data Flow Summary Table

| Stage | Input | Process | Output | Idempotency Mechanism |
|:---|:---|:---|:---|:---|
| **Transfer** | AWS S3 Parquet | S3ToGCSOperator | GCS Raw | N/A (source is immutable) |
| **Validation** | GCS Raw Parquet | Cloud Run (hash + schema check) | GCS Validated OR Quarantine | MD5 hash check in Firestore |
| **Processing** | GCS Validated Parquet | PySpark on Dataproc | GCS Processed (date-partitioned) | `partitionOverwriteMode=dynamic` |
| **Staging Load** | GCS Processed Parquet | BigQuery load job | BQ staging table | `WRITE_TRUNCATE` |
| **Merge** | BQ staging → BQ production | dbt MERGE on `order_id` | BQ core.enriched_orders | MERGE (upsert semantics) |

---

## 6. Interview Reference — One-Sentence Layer Descriptions

| Layer | One Sentence |
|:---|:---|
| **Bronze (GCS Raw)** | Immutable landing zone — exact copy of source files from AWS S3, never modified |
| **Cloud Run Validator** | Event-driven bouncer — checks schema and deduplicates every file before any compute runs on it |
| **Silver (GCS Processed)** | Enriched, deduplicated, type-safe Parquet — the output of PySpark, partitioned by date |
| **Gold (BigQuery)** | Analytics-ready, partitioned + clustered table — the source of truth for all Looker dashboards |
| **Cloud Composer** | The conductor — every task dependency, retry policy, and SLA check lives here |
| **Terraform** | Single source of truth for all infrastructure — every GCS bucket, IAM role, and BigQuery dataset is code |
| **Firestore** | Deduplication brain — stores MD5 hashes of every file ever processed to prevent double-processing |
| **dbt** | SQL transformation layer — handles the MERGE into production, schema evolution, and quality circuit breakers |
