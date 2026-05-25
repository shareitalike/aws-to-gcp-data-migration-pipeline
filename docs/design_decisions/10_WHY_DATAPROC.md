# Design Decision: Why Serverless Dataproc (PySpark) Instead of Dataflow or BigQuery SQL

**Category:** Processing Engine  
**Decision Date:** February 2024  
**Decision Owner:** Lead Data Engineer (Vipra Soft)  
**Status:** Approved & Implemented

---

## The Question

*"Why did you use Serverless Dataproc for your transformations? BigQuery can do joins and transformations natively. Cloud Dataflow is GCP's recommended processing service. Why PySpark?"*

---

## What We Chose

**Serverless Dataproc** running a PySpark job for the following operations:
- Multi-source join (orders + events + segments)
- Event pre-aggregation (prevent row explosion)
- Broadcast join for dimension table (segments)
- Deduplication by `order_id`
- Business rule filtering (zero-amount removal)
- Explicit type casting (schema normalization)
- Row count quality gate
- Date-partitioned Parquet output

---

## Alternatives We Evaluated

### Alternative 1: BigQuery SQL Only (Stored Procedures / Views)

You can absolutely do all of this inside BigQuery using stored procedures or multi-step SQL.

**What we considered:**
```sql
-- This is possible in BigQuery SQL
CREATE OR REPLACE TABLE core.enriched_orders AS
SELECT
    o.order_id, o.user_id, o.amount, o.process_date,
    s.user_segment,
    COUNT(e.event_type) AS event_count
FROM staging.orders_raw o
LEFT JOIN staging.user_segments s ON o.user_id = s.user_id
LEFT JOIN staging.events e ON o.user_id = e.user_id
WHERE o.amount > 0
GROUP BY 1,2,3,4,5;
```

**Why we rejected BigQuery SQL:**

1. **Row explosion was real**: A `JOIN` on `events` without pre-aggregation explodes rows. In BigQuery SQL, you can use `ARRAY_AGG` or subquery pre-aggregation, but this becomes unwieldy SQL that's hard to test and debug. In PySpark, `groupBy().agg()` is clean, explicit, and independently unit-testable.

2. **Row count quality gate is impossible in SQL without workarounds**: We needed to compare input row count to output row count and throw an exception if the ratio exceeded 1.1. In BigQuery SQL, you'd need a scripted stored procedure with `IF` statements and `RAISE` — brittle and hard to maintain. In PySpark, it's 3 lines of Python that we unit-tested.

3. **Cost at development/iteration time**: BigQuery charges per byte scanned. During development, running iterative SQL transformations on the full 6-month historical dataset (600GB) would have cost $3/TB × 600GB = **$1.80 per test run**. With dozens of iterations during development, this adds up. PySpark on a local machine or small Dataproc cluster costs cents.

4. **Unit testing gap**: SQL stored procedures cannot be unit-tested with PyTest. PySpark functions can be tested with `pyspark.sql.SparkSession` in a local test context. We had 12 unit tests for our Spark transformations — impossible to replicate for SQL procedures.

5. **Broadcast join control**: BigQuery's query planner does broadcast joins automatically for small tables, but it's opaque — you can't force it with a hint and verify it's happening. PySpark's `broadcast()` function is explicit and deterministic.

### Alternative 2: Cloud Dataflow (Apache Beam)

Dataflow is GCP's managed, serverless stream and batch processing service.

**What we considered:**
```python
# Dataflow / Apache Beam equivalent
with beam.Pipeline(options=options) as p:
    orders = p | 'ReadOrders' >> beam.io.ReadFromParquet(...)
    events = p | 'ReadEvents' >> beam.io.ReadFromParquet(...)
    enriched = (
        (orders, events)
        | 'Merge' >> beam.CoGroupByKey()
        | 'Enrich' >> beam.Map(enrich_fn)
    )
```

**Why we rejected Dataflow:**

1. **Overkill for batch**: Dataflow is the correct choice for streaming workloads. Our workload was once-daily batch. Using Dataflow for batch is like using a jet engine to power a bicycle — it works, but the operational overhead is unjustified.

2. **Beam programming model is more complex**: The Apache Beam model (PCollections, PTransforms, windowing, triggers) has a significant learning curve. Our team was already proficient in PySpark. Switching to Beam would have added 4–6 weeks of ramp-up time and introduced a steeper learning curve for the RetailEdge team we'd eventually hand off to.

3. **Startup latency for batch**: A Dataflow job has a 3–5 minute startup time before it starts processing data. For a once-daily job this is acceptable, but Serverless Dataproc also has ~90 second startup — faster.

4. **No Parquet native broadcast join**: Beam doesn't have a native equivalent of Spark's `broadcast()` hint. Side inputs can approximate it, but the code is more verbose and the optimization is less predictable.

5. **Development velocity**: Building and testing a Beam pipeline requires running the full pipeline or using DirectRunner (which doesn't accurately reflect distributed behavior). PySpark can run locally with a SparkSession for rapid development and exact same code runs on Dataproc.

---

## Why Serverless Dataproc Won

### Reason 1: Zero Cluster Operations

Traditional Dataproc required provisioning a cluster (N1 machines, disk, networking), managing it, and tearing it down — or paying for it 24/7. **Serverless Dataproc eliminates this entirely.**

```
Our job runtime: ~12–15 minutes per daily run
Cluster spin-up: ~90 seconds
Total billable time: ~16 minutes per day

Equivalent persistent cluster: 24 hours × 30 days = 720 hours/month
Serverless: 16 minutes × 30 = 480 minutes = 8 hours/month

Cost ratio: 8 hours vs 720 hours = ~90% cost reduction
```

### Reason 2: Team Already Knew PySpark

Our consulting team had 3 years of PySpark experience. RetailEdge's data engineering team had worked with Spark on AWS EMR. The code handoff was a knowledge transfer session, not a retraining program. This is a real business factor in consulting — using familiar tools reduces delivery risk.

### Reason 3: Exact Engineering Control

PySpark gave us explicit control over:
- `partitionOverwriteMode = dynamic` — overwrite only specific date partitions (critical for idempotency)
- `broadcast()` hints — guaranteed broadcast semantics for the segments table
- Explicit `.select()` — controlled schema output (our schema drift defense Layer 2)
- `row_number().over(Window)` — deduplication by partition key
- Unit-testable functions — PyTest suite with SparkSession fixture

### Reason 4: Native Parquet Integration

PySpark + Parquet is the most battle-tested combination in the data engineering ecosystem. Parquet schema self-description + Spark's lazy evaluation + AQE (Adaptive Query Execution) gave us:
- Automatic predicate pushdown (Spark reads only needed partitions)
- Columnar reads (Spark reads only needed columns)
- Runtime shuffle optimization (AQE reduces excessive shuffle partitions automatically)

---

## The Exact Comparison Table (Use in Interviews)

| Criterion | BigQuery SQL | Cloud Dataflow | Serverless Dataproc ✅ |
|:---|:---|:---|:---|
| **Learning curve** | Low (SQL) | High (Beam model) | Low (our team knew it) |
| **Development velocity** | Medium | Low | **High** |
| **Unit testability** | Very hard | Medium | **Easy (PyTest + SparkSession)** |
| **Row count quality gate** | Workaround needed | Possible | **3 lines of Python** |
| **Broadcast join control** | Opaque (BQ optimizer) | Side inputs (complex) | **Explicit broadcast() hint** |
| **Cluster management** | None (serverless) | None (serverless) | **None (serverless)** |
| **Cost model** | Bytes scanned | Hour-based | **Per-second, only when running** |
| **Streaming support** | With BQ Storage API | **Yes (native)** | Via Structured Streaming |
| **Parquet integration** | Native | Good | **Best-in-class** |
| **Ideal workload** | Ad-hoc SQL analytics | Streaming | **Batch ETL ✅** |

---

## Trade-offs of Our Chosen Approach

| Trade-off | Impact | Our Mitigation |
|:---|:---|:---|
| **Spark not ideal for streaming** | If client needed real-time, we'd need Dataflow | Phase 2 roadmap explicitly budgets for Dataflow migration |
| **90-second cold start** | Adds 90 sec to pipeline runtime | Our SLA is 6 hours — 90 seconds is 0.4% of the window |
| **PySpark skills needed for maintenance** | Client team must know Spark | Knowledge transfer sessions included; RetailEdge team had prior EMR experience |

---

## Interview Answer (Say This Out Loud)

> *"We explicitly evaluated all three options. Let me walk through why we ruled each one out.*
>
> *BigQuery SQL alone — you can do joins and enrichment inside BigQuery, but we had requirements that made it uncomfortable. We needed a row count quality gate that threw an exception if output rows exceeded input rows by more than 10% — that's 3 lines of Python, but a complex stored procedure in SQL. We also needed broadcast join control for our 200MB segments table, and we needed all our transformation logic to be unit-testable. SQL stored procedures can't be unit-tested with PyTest — PySpark functions can.*
>
> *Cloud Dataflow — Dataflow is the right tool for streaming. Our entire workload was once-daily batch. The Apache Beam programming model has a significant learning curve, and our team — including the RetailEdge engineers we'd eventually hand off to — were already proficient in PySpark. Using Dataflow would have added 4–6 weeks of ramp-up for no benefit.*
>
> *Serverless Dataproc won for three reasons: our team already knew PySpark so we moved fast; serverless meant we paid only for the ~16 minutes the job actually ran per day, not a persistent cluster; and PySpark's broadcast() hint, partitionOverwriteMode=dynamic, and explicit .select() gave us the engineering control we needed for idempotency, cost efficiency, and schema drift defense."*

---

## Related Documents
- [Why Cloud Composer](11_WHY_COMPOSER.md)
- [Spark Join Strategy](../engineering/21_SPARK_JOIN_STRATEGY.md)
- [Idempotency Design](../engineering/18_IDEMPOTENCY_DESIGN.md)
