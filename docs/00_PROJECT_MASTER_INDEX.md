# Project Master Index
### RetailEdge Global — GCP Data Platform | Vipra Soft Pvt Limited

> **Start here.** This document is your navigation guide to everything in this repository.

---

## 🎯 If You Have 5 Minutes (Before an Interview)

→ Open **[30_CHEAT_SHEET.md](interview_prep/30_CHEAT_SHEET.md)**

---

## 📖 If You're Preparing for an Interview (30–60 minutes)

1. **[25_OPENING_PITCH.md](interview_prep/25_OPENING_PITCH.md)** — Read all 3 versions out loud
2. **[24_MASTER_QA_GUIDE.md](interview_prep/24_MASTER_QA_GUIDE.md)** — Read questions marked 🔥
3. **[30_CHEAT_SHEET.md](interview_prep/30_CHEAT_SHEET.md)** — Review key numbers and one-liners

---

## 🛠️ If You Want to Build and Learn on GCP (Hands-on Lab)

→ Open **[32_HOW_TO_BUILD_ON_GCP.md](hands_on_lab/32_HOW_TO_BUILD_ON_GCP.md)** — Step-by-step guide to build, deploy, run, and tear down the pipeline on live GCP.

---

## 🏗️ If You Want to Understand the Architecture

1. **[05_SYSTEM_ARCHITECTURE.md](architecture/05_SYSTEM_ARCHITECTURE.md)** — Full ASCII architecture diagram
2. **[06_DATA_FLOW.md](architecture/06_DATA_FLOW.md)** — Trace one order record end-to-end with actual code
3. **[README.md](../README.md)** — Executive summary + how to run

---

## 💼 If You Want to Tell the Client Story

1. **[01_CLIENT_SCENARIO.md](consulting/01_CLIENT_SCENARIO.md)** — Client background, pain points, outcome
2. **[02_CONSULTING_STORY.md](consulting/02_CONSULTING_STORY.md)** — Your phase-by-phase engagement narrative
3. **[03_PROJECT_CHARTER.md](consulting/03_PROJECT_CHARTER.md)** — Scope, timeline, team, success criteria

---

## ⚙️ If You Want to Answer "Why Did You Choose X?"

| Question | Document |
|:---|:---|
| Why GCS intermediate layer? | [08_WHY_GCS_NOT_DIRECT.md](design_decisions/08_WHY_GCS_NOT_DIRECT.md) |
| Why Serverless Dataproc? | [10_WHY_DATAPROC.md](design_decisions/10_WHY_DATAPROC.md) |
| Why Cloud Composer? | [11_WHY_COMPOSER.md](design_decisions/11_WHY_COMPOSER.md) |
| Why partition + cluster BigQuery? | [16_PARTITIONING_CLUSTERING.md](design_decisions/16_PARTITIONING_CLUSTERING.md) |

---

## 🛡️ If You Want to Explain Engineering Decisions

| Topic | Document |
|:---|:---|
| Schema drift defense (3 layers) | [17_SCHEMA_DRIFT_DEFENSE.md](engineering/17_SCHEMA_DRIFT_DEFENSE.md) |
| Idempotency (every stage) | [18_IDEMPOTENCY_DESIGN.md](engineering/18_IDEMPOTENCY_DESIGN.md) |

---

## 💻 If You Want to Show the Code

| Component | File |
|:---|:---|
| Cloud Run Validator | [validate_landing.py](../02_ingestion/validate_landing.py) |
| PySpark Enrichment | [process_daily_orders.py](../03_processing/process_daily_orders.py) |
| BigQuery Load + MERGE | [load_and_merge.py](../04_warehouse/load_and_merge.py) |
| Airflow DAG | [retailedge_daily_pipeline.py](../06_orchestration/dags/retailedge_daily_pipeline.py) |
| dbt Model | [enriched_orders.sql](../05_transformation/models/core/enriched_orders.sql) |
| dbt Schema + Tests | [schema.yml](../05_transformation/models/core/schema.yml) |

---

## 📚 Complete Document Index

### Consulting Story
| # | Document | Contents |
|:---|:---|:---|
| 01 | [CLIENT_SCENARIO](consulting/01_CLIENT_SCENARIO.md) | Client background, pain points, volumes, outcomes |
| 02 | [CONSULTING_STORY](consulting/02_CONSULTING_STORY.md) | Phase-by-phase engagement narrative |
| 03 | [PROJECT_CHARTER](consulting/03_PROJECT_CHARTER.md) | Scope, timeline, team, risks, success criteria |

### Architecture
| # | Document | Contents |
|:---|:---|:---|
| 05 | [SYSTEM_ARCHITECTURE](architecture/05_SYSTEM_ARCHITECTURE.md) | Full pipeline ASCII diagram + component map |
| 06 | [DATA_FLOW](architecture/06_DATA_FLOW.md) | Single order traced through every layer with code |

### Design Decisions
| # | Document | Contents |
|:---|:---|:---|
| 08 | [WHY_GCS_NOT_DIRECT](design_decisions/08_WHY_GCS_NOT_DIRECT.md) | Why GCS intermediate vs direct S3→BQ |
| 10 | [WHY_DATAPROC](design_decisions/10_WHY_DATAPROC.md) | Dataproc vs Dataflow vs BigQuery SQL |
| 11 | [WHY_COMPOSER](design_decisions/11_WHY_COMPOSER.md) | Cloud Composer vs Cloud Scheduler |
| 16 | [PARTITIONING_CLUSTERING](design_decisions/16_PARTITIONING_CLUSTERING.md) | BQ partition + cluster cost analysis |

### Engineering Deep Dives
| # | Document | Contents |
|:---|:---|:---|
| 17 | [SCHEMA_DRIFT_DEFENSE](engineering/17_SCHEMA_DRIFT_DEFENSE.md) | Three-layer schema drift defense with code |
| 18 | [IDEMPOTENCY_DESIGN](engineering/18_IDEMPOTENCY_DESIGN.md) | Idempotency at every pipeline stage |

### Interview Preparation
| # | Document | Contents |
|:---|:---|:---|
| 24 | [MASTER_QA_GUIDE](interview_prep/24_MASTER_QA_GUIDE.md) | 25+ Q&A covering all topics |
| 25 | [OPENING_PITCH](interview_prep/25_OPENING_PITCH.md) | Three versions of your opening pitch |
| 30 | [CHEAT_SHEET](interview_prep/30_CHEAT_SHEET.md) | ⭐ Read this 5 minutes before any interview |
| 33 | [OCI_JD_MAPPING](interview_prep/33_OCI_JD_MAPPING.md) | Mapping guide for Oracle OCI Medallion roles |
| 34 | [DATABRICKS_1HR_PIPELINE](interview_prep/34_DATABRICKS_1HR_PIPELINE.md) | Architecture for 1-hour SLA micro-batch in Databricks |
| 35 | [DELTA_LIVE_TABLES](interview_prep/35_DELTA_LIVE_TABLES.md) | DLT concepts, Expectations, and CDC |
| 36 | [DATABRICKS_TIME_TRAVEL](interview_prep/36_DATABRICKS_TIME_TRAVEL.md) | How to restore data and the VACUUM limitation |
| 37 | [DATABRICKS_STREAMING_OPTIMIZATION](interview_prep/37_DATABRICKS_STREAMING_OPTIMIZATION.md) | Bronze-Gold streaming setup and performance optimizations |
| 38 | [DATABRICKS_AIRFLOW_INTEGRATION](interview_prep/38_DATABRICKS_AIRFLOW_INTEGRATION.md) | How Airflow orchestrates Databricks via REST API |
| 39 | [VIBE_CODING_MCP_GUARDRAILS](interview_prep/39_VIBE_CODING_MCP_GUARDRAILS.md) | Answering questions about AI agents and MCP |
| 40 | [DATA_VISUALIZATION_BI_PRIMER](interview_prep/40_DATA_VISUALIZATION_BI_PRIMER.md) | How BI tools connect to BigQuery and caching |
| 41 | [S3_TO_GCS_OPERATOR](interview_prep/41_S3_TO_GCS_OPERATOR.md) | Airflow data transfer mechanisms and architectural limits |
| 42 | [GCS_EVENTARC_TRIGGER](interview_prep/42_GCS_EVENTARC_TRIGGER.md) | How Eventarc triggers Cloud Run from GCS uploads |
| 44 | [WHY_GCP_OVER_AWS](interview_prep/44_WHY_GCP_OVER_AWS.md) | Architectural defense and service mapping (AWS vs GCP) |
| 45 | [DEFENSE_MECHANISMS](interview_prep/45_DEFENSE_MECHANISMS.md) | Setting boundaries for Kafka and BI Dashboards |
| 46 | [GCP_COST_ANALYSIS](interview_prep/46_GCP_COST_ANALYSIS.md) | Proving GCP cost savings through a PoC |
| 47 | [ATHENA_VS_REDSHIFT](interview_prep/47_ATHENA_VS_REDSHIFT.md) | Data Lake vs Data Warehouse query engines |
| 48 | [BIGQUERY_VS_ATHENA_COST](interview_prep/48_BIGQUERY_VS_ATHENA_COST.md) | Defending the $6.25/TB BQ cost vs $5.00/TB Athena cost |
| 49 | [KAFKA_RELIABILITY_DEFENSE](interview_prep/49_KAFKA_RELIABILITY_DEFENSE.md) | Safely answering Kafka broker config questions |

### Hands-on Lab
| # | Document | Contents |
|:---|:---|:---|
| 32 | [HOW_TO_BUILD_ON_GCP](hands_on_lab/32_HOW_TO_BUILD_ON_GCP.md) | Full infra deployment, Airflow config, and PySpark execution |
| 43 | [HANDS_ON_EVENTARC_TRIGGER](hands_on_lab/43_HANDS_ON_EVENTARC_TRIGGER.md) | Step-by-step guide to building the GCS to Cloud Run trigger with gcloud CLI commands |
