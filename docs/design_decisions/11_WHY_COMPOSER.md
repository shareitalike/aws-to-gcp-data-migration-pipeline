# Design Decision: Why Cloud Composer (Airflow) Instead of Cloud Scheduler + Cloud Run

**Category:** Orchestration  
**Decision Date:** March 2024  
**Decision Owner:** Lead Data Engineer  
**Status:** Approved & Implemented

---

## The Question

*"Why did you use Cloud Composer? It's expensive — around $300–400/month for a small environment. Cloud Scheduler with Cloud Run would cost almost nothing. What justified Composer?"*

---

## What We Chose

**Cloud Composer 2 (managed Airflow 2.8)** running a single DAG: `retailedge_daily_pipeline`

DAG structure:
```
transfer_s3_to_gcs → check_file_validation → trigger_dataproc_spark
→ load_bq_staging → run_dbt_incremental → pipeline_success_notification
```

With on each task:
- Per-task retry policies (3 retries, 5-minute exponential backoff)
- `on_failure_callback` → Slack alert
- `sla` → alert if task exceeds 30 minutes
- `{{ ds }}` logical date passed to every downstream system

---

## Alternatives We Evaluated

### Alternative 1: Cloud Scheduler + Cloud Run (Cheapest Option)

**Architecture:**
```
Cloud Scheduler (cron: 0 0 * * *) → HTTP POST to Cloud Run
Cloud Run (orchestrator container):
  step 1: call S3 transfer API
  step 2: if success, call Dataproc API
  step 3: if success, call BQ load API
  step 4: if success, call dbt Cloud API
  step 5: on any failure, call Slack webhook
```

**Cost:** ~$5–10/month total

**Why we rejected it:**

1. **No native dependency management**: Cloud Scheduler fires once. If step 2 (Dataproc) takes 15 minutes, your Cloud Run container has to poll Dataproc's API in a loop. That's manual polling logic you write and maintain. And Cloud Run has a 3,600-second max timeout — fine for us, but a hard architectural limit.

2. **No conditional branching**: Our pipeline needed: *If the file validator found zero valid files → skip Dataproc entirely (no input = Spark exception)*. In Cloud Run, this is an `if/else` inside your Python script — not visually testable and not monitored by any system. In Airflow, it's a `BranchPythonOperator` — a first-class citizen.

3. **No task-level retry with state**: If step 3 (BQ load) fails and you need to retry only from that point, Cloud Scheduler would re-trigger the entire pipeline from step 1. That means re-running the S3 transfer (waste), re-running Spark (waste), and potentially double-loading the BQ staging. You'd have to build your own "checkpoint" state machine — probably in Firestore — to know where you were.

4. **No UI**: When something fails at 2 AM, the on-call engineer opens a browser. With Cloud Scheduler, they're looking at Cloud Logging for specific log messages in a wall of text. With Airflow, they open the Composer UI, see a visual DAG with red and green tasks, click the failed task, read its logs, click "Clear & Retry" — done. **The operational visibility alone saved 30–60 minutes per incident.**

5. **No backfill capability**: If the pipeline fails for 3 consecutive days (say, S3 is down), you need to replay those 3 days. With Cloud Scheduler, you manually trigger the Cloud Run container 3 times with the right date parameters and pray. With Airflow, `airflow dags backfill --start-date 2025-06-01 --end-date 2025-06-03 retailedge_daily_pipeline`.

6. **SLA monitoring is DIY**: We had a hard SLA — data must be available by 06:00 IST. With Cloud Scheduler, you'd need to write a separate monitoring Cloud Run that checks if a success log was written by 06:00 and fires Slack if not. That's a monitoring pipeline to monitor your pipeline. In Airflow, `sla=timedelta(hours=4)` on the DAG does this natively.

### Alternative 2: Cloud Workflows

Google Cloud Workflows is a serverless, low-cost orchestration service for multi-step workflows.

**Cost:** ~$0.01/1,000 steps — essentially free

**Why we rejected it:**

1. **YAML-based workflow definition**: Cloud Workflows uses YAML with Google's proprietary syntax. It's powerful for simple sequential workflows but lacks Airflow's Python expressiveness for complex conditional logic.

2. **No DAG visualisation**: Cloud Workflows has a basic execution graph but it's nowhere near Airflow's Gantt chart view, task log viewer, and historical run comparison.

3. **No Dataproc native operator**: We'd have to implement a custom step that polls the Dataproc API. Airflow's `DataprocSubmitJobOperator` handles this natively — including automatic polling, timeout, and state reporting.

4. **Limited retry semantics**: Cloud Workflows supports retries but not the rich retry-with-exponential-backoff-and-SLA-callback model that Airflow provides.

---

## Why Cloud Composer Won

### Reason 1: Task Dependency Graph (The Core Problem)

Our pipeline has **conditional dependencies**:

```python
# Airflow ShortCircuitOperator
check_valid_files = ShortCircuitOperator(
    task_id='check_file_validation',
    python_callable=check_any_valid_files,  # Returns False if no valid files
)

# If check_valid_files returns False, ALL downstream tasks are SKIPPED
# Not failed — skipped. The DAG shows yellow (skipped), not red (failed)
# No Slack alert fires, because this is a legitimate no-data scenario
trigger_spark.set_upstream(check_valid_files)
```

This conditional skip logic is **impossible** to do cleanly with Cloud Scheduler.

### Reason 2: Partial Recovery (The Most Valuable Feature)

When `load_bq_staging` failed at 00:31 on Day 45 of production:
1. On-call engineer saw Slack alert at 00:31
2. Opened Composer UI — saw DAG with `transfer_s3_to_gcs` ✅, `trigger_spark` ✅, `load_bq_staging` ❌
3. Read the failure log in the UI: "BigQuery API quota exceeded, retry in 5 minutes"
4. Clicked "Clear" on `load_bq_staging` — this clears it and all downstream tasks
5. Airflow re-ran from exactly `load_bq_staging` — no re-transfer, no re-Spark
6. Total intervention: **4 minutes, 0 lines of code, 0 SQL queries**

With Cloud Scheduler: re-trigger from the start, re-run Spark (15 minutes wasted), re-load staging, re-run dbt. **~20 extra minutes of compute cost per incident.**

### Reason 3: `{{ ds }}` Logical Date (Production-Critical Correctness)

```python
DataprocSubmitJobOperator(
    task_id='trigger_dataproc_spark',
    job={
        "pyspark_job": {
            "main_python_file_uri": "gs://retailedge-code/process_daily_orders.py",
            "args": ["--date", "{{ ds }}"],  # Always the logical execution date
        }
    }
)
```

When we retried a failed March 19th run on March 22nd, `{{ ds }}` still returned `2026-03-19`. Spark wrote to the `process_date=2026-03-19` partition. The MERGE corrected March 19th's data. Perfect.

With `datetime.today()` in a Cloud Run script: **we would have processed March 22nd's data when retrying March 19th — permanently wrong analytics.**

### Reason 4: Cost Amortisation Across Multiple Pipelines

This was the deciding business argument with the client's CFO:

| Consideration | Cloud Scheduler | Cloud Composer |
|:---|:---|:---|
| Cost for 1 pipeline | ~$5/month | **~$350/month** |
| Cost for 8 pipelines | ~$40/month | **~$350/month (same instance)** |

RetailEdge had 7 more pipelines planned (inventory, pricing, customer analytics). A single Cloud Composer environment runs all of them. The cost per pipeline drops from $350 to $44/month as more pipelines are added — **same cost as Cloud Scheduler with enterprise-grade orchestration.**

---

## Honest Trade-offs

| Trade-off | Impact | Our Mitigation |
|:---|:---|:---|
| **Cost: ~$350/month baseline** | Significant for small orgs | Justified by 8+ planned pipelines, ROI from incident reduction |
| **Composer environment startup: 20–30 min** | One-time setup cost | Not a recurring concern |
| **Airflow learning curve** | Team must know Airflow | RetailEdge team trained; included in knowledge transfer |
| **GKE-based (more complex infrastructure)** | Composer runs on GKE pods | Fully managed by Google — no GKE ops required from us |

---

## When I Would Use Cloud Scheduler Instead

I'd use Cloud Scheduler + Cloud Run for:
- A single, linear pipeline with no conditional branches
- A pipeline with no retry requirements (fire-and-forget)
- A pipeline where backfill is never needed
- A startup with minimal budget and a single simple workflow

For RetailEdge — with conditional logic, 30-minute SLA monitoring, backfill requirements, and 8 planned pipelines — **Composer was the right call.**

---

## Interview Answer (Say This Out Loud)

> *"You're right that Cloud Composer is not cheap — it's around $300–350 a month for a small environment. And Cloud Scheduler + Cloud Run would cost maybe $5. So I had to justify it.*
>
> *Cloud Scheduler with Cloud Run just cannot handle what we needed. Our pipeline had conditional logic: if the validator found zero valid files, we needed to skip Spark entirely — because running Spark with no input throws exceptions. If the Spark row count gate failed, skip the BigQuery load. If dbt tests failed, alert and stop. Managing those conditional branches in a Cloud Run script means writing your own state machine — re-inventing the wheel.*
>
> *What Composer gave us out of the box: task-level dependency graph, per-task retries with backoff, SLA miss callbacks, a visual UI where any engineer could see exactly which task failed, click Clear, and retry from that exact point. And the `{{ ds }}` logical date template — without this, a retry on March 22nd for March 19th's pipeline would silently process the wrong day. That's a fundamental correctness guarantee.*
>
> *The business justification: the client had 8 pipelines planned. One Composer environment handles all of them. Amortised across 8 pipelines, the cost drops to $44 per pipeline — comparable to Cloud Scheduler with none of the operational overhead."*

---

## Related Documents
- [System Architecture](../architecture/05_SYSTEM_ARCHITECTURE.md)
- [Idempotency Design](../engineering/18_IDEMPOTENCY_DESIGN.md)
- [Why Dataproc](10_WHY_DATAPROC.md)
