# RetailEdge Platform — Incident Response Runbook

**Audience:** Data Engineering / On-Call Engineer  
**System:** AWS-to-GCP Retail Analytics Platform  
**Alert Channel:** `#alerts-data-platform` (Slack)  

---

## 🔴 ALERT: File Quarantined (Schema Validation Failed)

**Slack Message Example:**
> 🚨 **QUARANTINE ALERT**  
> **File:** `orders_2026-03-15.parquet`  
> **Reason:** Schema mismatch. Missing required column: `amount`  
> **Location:** `gs://retailedge-landing-quarantine-prod/orders_2026-03-15.parquet`

**Triage Steps:**
1. **Acknowledge the alert** in Slack with an 👀 emoji so the team knows you are looking at it.
2. **Verify the failure:** Run a quick PyArrow check locally or via a Colab notebook against the quarantined file to confirm the `amount` column is truly missing.
3. **Escalate to upstream:** Contact the AWS OMS engineering team via the `#data-producers-oms` channel. 
   - *Message:* "Hi team, today's order file dropped the `amount` column. Can you please investigate and re-export the file?"
4. **No immediate pipeline action required:** Because the file was routed to Quarantine, the downstream Spark and BigQuery jobs are safe. The staging and production tables have not been corrupted.

**Resolution Steps (Once upstream provides the fixed file):**
1. The upstream team drops the fixed file into the GCS Raw bucket.
2. EventArc automatically triggers Cloud Run.
3. The file passes validation and is routed to Validated.
4. The daily Airflow DAG picks it up automatically.

---

## 🔴 ALERT: Spark Row Explosion Quality Gate Failed

**Slack Message Example:**
> 🚨 **PIPELINE HALTED**  
> **Task:** `spark_enrichment_job`  
> **Reason:** Row count explosion detected. Input: 100,000, Output: 180,000. Ratio 1.8 > 1.10.

**Triage Steps:**
1. **Acknowledge the alert** in Slack.
2. **Check the Airflow logs:** Open the failed Airflow task logs. Verify the exact input and output counts printed by the Spark quality gate.
3. **Investigate the Join:** This almost always means the dimension table (`user_segments`) has duplicate IDs, causing a Cartesian product during the broadcast join.
4. **Query the Validated bucket:** Use Athena or BigQuery external tables to query the raw segments file:
   ```sql
   SELECT user_id, COUNT(*) 
   FROM raw_segments 
   GROUP BY user_id 
   HAVING COUNT(*) > 1
   ```
5. **Resolution:** If duplicates are found in the segments file, contact the CRM team to fix their export. Once fixed, clear the failed Airflow task to restart the Spark job.

---

## 🔴 ALERT: dbt Data Contract Test Failed

**Slack Message Example:**
> 🚨 **dbt TEST FAILURE**  
> **Model:** `core.enriched_orders`  
> **Test:** `not_null_amount`  
> **Failures:** 14 records  

**Triage Steps:**
1. **Acknowledge the alert** in Slack.
2. **Check the production table:** The dbt test runs *after* the MERGE. This means 14 rows with null amounts made it into the production table.
3. **Query the staging table:** Check if the null amounts exist in staging. If they do, the bug is in the PySpark enrichment logic (which is supposed to filter or default null amounts).
4. **Resolution:** 
   - Write a hotfix for `process_daily_orders.py` to correctly handle the nulls.
   - Deploy the hotfix.
   - Run a backfill for today's date (see Operational Runbook) to overwrite the bad data in BigQuery.
