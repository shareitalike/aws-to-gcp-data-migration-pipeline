# RetailEdge Platform — Operational Runbook

**Audience:** Data Engineering / Operations Team  
**System:** AWS-to-GCP Retail Analytics Platform  
**Last Updated:** March 2026 (Handoff)  

---

## 1. How to Run a Historical Backfill

**Scenario:** The business needs to reload a specific historical date (e.g., June 1, 2025) due to a late-arriving data correction from the upstream OMS team.

**Prerequisites:**
1. Ensure the corrected raw files are present in the GCS Raw bucket under the target date folder.

**Execution Steps:**
1. Open the **Cloud Composer (Airflow)** UI.
2. Navigate to the `retailedge_daily_pipeline` DAG.
3. In the top-right corner, click **Trigger DAG w/ config**.
4. Pass the logical date as a parameter:
   ```json
   {"logical_date": "2025-06-01"}
   ```
5. Click **Trigger**.

**What happens automatically (Idempotency guarantee):**
- The pipeline will NOT overwrite today's data.
- Spark's `partitionOverwriteMode=dynamic` ensures it only overwrites the GCS partition for `process_date = '2025-06-01'`.
- BigQuery's dbt MERGE statement will safely UPDATE existing rows for that date and INSERT new ones.

---

## 2. How to Add a New Approved Column

**Scenario:** The business requested a new column `promo_code` to be added to the daily orders feed. The upstream OMS team has agreed to start sending it tomorrow.

**Execution Steps:**

**Step 1: Update the Cloud Run Contract (Layer 1)**
1. Open the Cloud Run source repo (`validate_landing.py`).
2. Add the column to the PyArrow expected schema:
   ```python
   EXPECTED_SCHEMA = pa.schema([
       ...
       ('promo_code', pa.string())  # NEW
   ])
   ```
3. Commit and push. CI/CD will redeploy the Cloud Run container.

**Step 2: Update the PySpark Output Filter (Layer 2)**
1. Open `process_daily_orders.py`.
2. Add the column to the approved output list:
   ```python
   APPROVED_OUTPUT_COLUMNS = [
       ...
       "promo_code" # NEW
   ]
   ```
3. Commit and push.

**Step 3: Update the dbt Contract (Layer 3)**
1. Open `models/core/schema.yml`.
2. Add the column documentation and tests:
   ```yaml
     - name: promo_code
       description: "Discount code applied at checkout"
   ```
3. Open `models/core/enriched_orders.sql` and add the column to the SELECT statement.
4. Commit and push.

---

## 3. How to Pause the Pipeline for Upstream Outages

**Scenario:** The AWS OMS system is down and sending corrupt empty files. We need to pause ingestion.

**Execution Steps:**
1. Open the **Cloud Composer (Airflow)** UI.
2. Find the `retailedge_daily_pipeline` DAG.
3. Toggle the switch on the far left to **OFF (Pause)**.
4. Once the upstream team confirms the outage is resolved, toggle back to **ON (Unpause)**.
5. The Airflow scheduler will automatically catch up on missed intervals sequentially. Do not manually trigger past runs unless specifically required.
