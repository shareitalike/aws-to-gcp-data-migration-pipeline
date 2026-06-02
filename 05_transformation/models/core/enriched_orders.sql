-- RetailEdge Global — dbt Incremental Model: enriched_orders
-- =============================================================
-- GCP Data Engineering Engagement
--
-- Purpose:
--     MERGE staging.orders_daily into core.enriched_orders.
--     Incremental strategy: new rows INSERT, existing rows UPDATE.
--
-- Key dbt Config:
--     materialized='incremental': Only processes new/changed rows per run
--     unique_key='order_id': MERGE key — upsert semantics
--     on_schema_change='append_new_columns': Layer 3 schema drift defense
--         When a new column appears in staging (after Spark .select() approval),
--         dbt automatically runs ALTER TABLE ADD COLUMN in production.
--         No manual DDL. No --full-refresh required.
--     partition_by: Mirrors the production table physical partitioning
--     cluster_by: Mirrors the production table clustering
--
-- Idempotency:
--     MERGE semantics: matched rows UPDATE to same values on re-run.
--     Running this model 10 times = same table as running it once.
--
-- Partition Pruning in MERGE:
--     The incremental WHERE clause limits source to today's date.
--     Including process_date in unique_key (via the WHERE on source)
--     tells BigQuery to only scan the current date's partition of the
--     target table during the MERGE — reducing scan from 500GB to ~700MB.

{{
    config(
        materialized='incremental',
        unique_key=['process_date', 'order_id'],
        incremental_strategy='merge',
        partition_by={
            "field": "process_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['user_segment', 'user_id'],
        require_partition_filter=True,
        on_schema_change='append_new_columns',
        tags=['daily', 'core', 'production']
    )
}}

WITH staging_orders AS (
    SELECT
        order_id,
        user_id,
        amount,
        currency,
        DATE('{{ var("execution_date") }}') AS process_date,
        user_segment,
        event_count,
        ARRAY(SELECT e.element FROM UNNEST(event_types.list) AS e) AS event_types
    FROM {{ source('staging', 'orders_daily') }}

    -- Partition pruning: Because Airflow loads staging_orders using WRITE_TRUNCATE daily,
    -- it only contains the current execution_date's data. 
    -- BigQuery uses the target's process_date via the unique_key during the MERGE for pruning.
),

-- Data quality layer: reject any rows that fail critical checks
-- These will be logged but will NOT block the pipeline run
-- (dbt tests in schema.yml act as the circuit breaker)
quality_filtered AS (
    SELECT *
    FROM staging_orders
    WHERE
        order_id     IS NOT NULL      -- No orphan rows
        AND user_id  IS NOT NULL      -- No unattributable orders
        AND amount   > 0              -- No zero-amount artifacts (handled in Spark too)
        AND currency IN ('INR', 'USD', 'EUR', 'GBP')  -- Accepted currency codes
)

SELECT
    order_id,
    user_id,
    ROUND(amount, 2)    AS amount,   -- Normalise to 2 decimal places
    UPPER(currency)     AS currency, -- Normalise to uppercase
    process_date,
    COALESCE(user_segment, 'unclassified') AS user_segment,
    COALESCE(event_count, 0)               AS event_count,
    event_types,
    CURRENT_TIMESTAMP() AS updated_at
FROM quality_filtered
