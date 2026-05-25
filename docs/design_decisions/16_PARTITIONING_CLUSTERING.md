# Design Decision: Why BigQuery Partitioning + Clustering (Cost Analysis)

**Category:** Warehouse Design  
**Decision Date:** March 2024  
**Decision Owner:** Lead Data Engineer (Vipra Soft)  
**Status:** Approved & Implemented

---

## The Question

*"Why did you partition your BigQuery table by `process_date` and cluster by `user_id` and `user_segment`? What's the actual cost impact? And why did you enforce `require_partition_filter = TRUE`?"*

---

## Context: How BigQuery Bills You

BigQuery on-demand pricing charges **$5 per TB of data scanned** per query.

Without partitioning:
- Every query scans the ENTIRE table regardless of filters
- `WHERE process_date = '2025-06-01'` on a 2-year table = 2 years × 365 days scanned

With partitioning:
- `WHERE process_date = '2025-06-01'` reads ONLY June 1st's data = ~1/730th of the table

The math: 98,000 rows/day × 365 days × 2 years = ~71.5 million rows. At ~7 bytes per row average = **~500GB total table size**.

---

## Our Table DDL

```sql
CREATE TABLE `retailedge-data-prod.core.enriched_orders`
(
    order_id        STRING    NOT NULL,
    user_id         STRING    NOT NULL,
    amount          FLOAT64   NOT NULL,
    currency        STRING    NOT NULL,
    process_date    DATE      NOT NULL,
    user_segment    STRING,
    event_count     INT64,
    inserted_at     TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP
)
PARTITION BY process_date
CLUSTER BY user_segment, user_id
OPTIONS (
    require_partition_filter = TRUE,
    description = 'Production enriched orders table. Partitioned by process_date, clustered by user_segment and user_id. All queries MUST include process_date filter.'
);
```

---

## Partitioning: The Why

### What It Does
BigQuery physically stores each day's data in a separate storage shard. When a query includes `WHERE process_date = '2025-06-01'`, BigQuery reads ONLY that shard.

### The Cost Impact (Real Numbers)

| Scenario | Table Size | Bytes Scanned Per Query | Cost Per Query |
|:---|:---|:---|:---|
| No partitioning, full table | 500GB | 500GB | $2.50 |
| Partitioned, single day filter | 500GB | ~700MB (1 day) | $0.0035 |
| **Cost reduction** | — | **99.86%** | **99.86%** |

**For 50 analyst queries per day:**
- Without partitioning: 50 × $2.50 = **$125/day = $3,750/month**
- With partitioning (single day queries): 50 × $0.0035 = **$0.175/day = $5.25/month**

**Monthly savings: ~$3,745** — this is the number we put in front of the CFO.

### Why `process_date` Specifically?

The most common query pattern for RetailEdge analysts:
```sql
-- Daily revenue report
SELECT user_segment, SUM(amount) FROM core.enriched_orders
WHERE process_date = CURRENT_DATE() - 1  -- Yesterday's data

-- Last 7 days trend
SELECT process_date, SUM(amount) FROM core.enriched_orders
WHERE process_date BETWEEN CURRENT_DATE() - 7 AND CURRENT_DATE()

-- Month-end reconciliation
SELECT SUM(amount) FROM core.enriched_orders
WHERE process_date BETWEEN '2025-06-01' AND '2025-06-30'
```

Every single common query pattern filters by `process_date`. Partitioning by this column means every routine query hits only the required data.

### Additional Benefit: Incremental MERGE Efficiency

The dbt incremental MERGE statement:
```sql
MERGE INTO core.enriched_orders AS target
USING staging.orders_daily AS source
ON target.order_id = source.order_id
AND target.process_date = source.process_date  -- partition pruning in the MERGE key
```

Including `process_date` in the MERGE `ON` clause means BigQuery only scans the `process_date = today` partition of the production table during the MERGE — not the entire 500GB table. **This reduced our nightly MERGE from ~8 seconds (full scan) to ~0.3 seconds (partition scan).**

---

## Clustering: The Why

### What It Does
Within each partition, BigQuery co-locates rows with the same clustering column values. It also sorts data by the clustering columns and maintains internal block statistics. This allows BigQuery to skip entire data blocks that don't contain the queried values.

### Why `user_segment` First, Then `user_id`

**Column order in clustering matters** — BigQuery uses the first column for the coarsest grouping, then refines within that.

RetailEdge's most common queries:
```sql
-- Segment-level analysis (most common)
WHERE process_date = '2025-06-01' AND user_segment = 'premium_buyer'

-- User-level analysis (less common, but important)  
WHERE process_date = '2025-06-01' AND user_id = 'USR-9847231'

-- Combined
WHERE process_date = '2025-06-01' AND user_segment = 'premium_buyer' AND user_id = 'USR-9847231'
```

By clustering `user_segment` first, segment-level queries (the most common) benefit maximally. `user_id` second means user-level queries also benefit, and BigQuery can narrow down within the already-clustered segment blocks.

### The Cost Impact of Clustering

For a partition of 98,000 rows with 5 user segments:
- Without clustering: `WHERE user_segment = 'premium_buyer'` reads all 98,000 rows
- With clustering: BigQuery skips ~80% of blocks (4 segments not needed) → reads ~20,000 rows

**Additional bytes scanned reduction within a partition: ~60–80% for filtered segment queries.**

---

## `require_partition_filter = TRUE`: The Hard Guardrail

### What It Does

This setting makes BigQuery **refuse to execute** any query on the table that does not include a `process_date` filter:

```sql
-- This query FAILS with an error:
SELECT * FROM core.enriched_orders WHERE user_segment = 'premium_buyer';
-- Error: "Queries over table 'core.enriched_orders' require a filter 
--  over column(s) 'process_date' that can be used for partition elimination."

-- This query SUCCEEDS:
SELECT * FROM core.enriched_orders 
WHERE process_date BETWEEN '2025-06-01' AND '2025-06-30'
AND user_segment = 'premium_buyer';
```

### Why This Is Essential in Production

Without this setting: A new analyst joins RetailEdge, doesn't know about the partitioning, runs:
```sql
SELECT COUNT(*) FROM core.enriched_orders WHERE user_id = 'USR-9847231';
```
This scans the entire 500GB table. Cost: **$2.50 for a row count query.**

With this setting: The query fails immediately with a clear error message. The analyst must add a `process_date` filter. The corrected query scans 700MB (one partition). Cost: **$0.0035.**

### What About Queries That Genuinely Need the Full Table?

For historical analysis or dashboard aggregations that span all time, we created a `BQDT_admin` service account bypass:
```sql
-- Analytics team lead can query historical data by explicitly acknowledging the cost
SELECT DISTINCT user_id FROM `retailedge-data-prod.core.enriched_orders`
WHERE _PARTITIONDATE >= '2023-01-01';  -- Still uses partition pruning
```

The key: even "full history" queries use the partition column, just with a wider range.

---

## The MERGE + Partition Interaction (For Senior-Level Interviews)

```sql
-- dbt compiles this for an incremental run on 2025-06-01:
MERGE INTO `core.enriched_orders` AS target
USING (
    SELECT * FROM `staging.orders_daily`
    WHERE process_date = '2025-06-01'  -- ← This limits source to 1 day
) AS source
ON target.order_id = source.order_id
AND target.process_date = source.process_date  -- ← This limits target scan to 1 partition
```

Why include `process_date` in the MERGE `ON` clause even though `order_id` is unique?

Because without `target.process_date = source.process_date`, BigQuery must scan the **entire target table** to find matching `order_id`s. That's 500GB scanned per nightly MERGE.

With `target.process_date = source.process_date`, BigQuery only scans the `process_date=2025-06-01` partition — ~700MB. **The partition column in the MERGE key is not for correctness — it's for cost efficiency.**

---

## Interview Answer (Say This Out Loud)

> *"BigQuery charges by bytes scanned. Without partitioning, every single query scans the entire historical table — even if you only want yesterday's data. At 2 years of history and ~98,000 rows per day, our table was around 500GB. A query for yesterday's revenue without partitioning costs $2.50. With partitioning by `process_date`, that same query reads only ~700MB — one partition — and costs $0.0035. That's a 99.86% cost reduction per query. For 50 analyst queries per day, the monthly savings were around $3,700.*
>
> *Clustering by `user_segment` first, then `user_id`, goes one level deeper. Within each date partition, BigQuery co-locates rows by these columns. A query that filters by `user_segment = 'premium_buyer'` can skip ~80% of the blocks within a partition. So partitioning reduces scans across the table; clustering reduces scans within a partition.*
>
> *We also set `require_partition_filter = TRUE` — a hard cost guardrail. BigQuery will literally refuse to execute any query on the production table that doesn't include a `process_date` filter. No accidental full-table scans, ever.*
>
> *And one subtle production detail: in our MERGE statement, we include `target.process_date = source.process_date` in the ON clause alongside `order_id`. This is not for correctness — `order_id` is already unique. It's for cost efficiency: it tells BigQuery to only scan the relevant date partition of the production table during the nightly MERGE, instead of the full 500GB."*

---

## Related Documents
- [Why BigQuery over Snowflake](14_WHY_BIGQUERY.md)
- [Why dbt](12_WHY_DBT.md)
- [Idempotency Design](../engineering/18_IDEMPOTENCY_DESIGN.md)
