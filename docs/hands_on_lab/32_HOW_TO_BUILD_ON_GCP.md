# GCP Hands-On Implementation Lab Guide
### Build the RetailEdge Global Pipeline from Scratch | Vipra Soft Pvt Limited

This document is your step-by-step practical guide to building, configuring, deploying, and running the entire AWS-to-GCP RetailEdge data pipeline on a live Google Cloud Platform (GCP) account. 

By building this yourself, you will gain hands-on experience with:
*   **Google Cloud Storage (GCS)** & **Firestore** (landing zone & state registry)
*   **Cloud Run** & **Eventarc** (serverless, event-driven validation gate)
*   **Dataproc Serverless** (serverless PySpark job execution)
*   **BigQuery** (partitioned, clustered data warehousing with MERGE upserts)
*   **dbt Core** (data transformations and analytics test suites)
*   **Cloud Composer** (orchestration with Managed Apache Airflow)

---

## 🗺️ Lab Overview & Architecture Flow

```
[Local Raw CSVs] 
      │ (Generate Sample Parquet)
      ▼
[gs://retailedge-landing-raw-prod]  ◄── GCS Object Created Event
      │ 
      ▼
[Eventarc Trigger]
      │ (HTTPS POST)
      ▼
[Cloud Run Validator] ──(Checks Firestore Dedup Hash)
      │ 
      ├──► VALID: [gs://retailedge-landing-validated-prod]
      └──► INVALID: [gs://retailedge-landing-quarantine-prod] (+ Slack Alert)
            │
            ▼
     [Cloud Composer (Airflow) DAG] (Daily Schedule / Manual Trigger)
            │
            ├──► Step 1: Submit Dataproc Serverless PySpark Job
            │            (Reads gs://...validated-prod/orders, events, segments)
            │            (Joins & Enrichments, writes gs://retailedge-processed-prod/)
            │
            ├──► Step 2: Run load_and_merge.py (BigQuery Staging Load + MERGE Upsert)
            │            (Loads staging.orders_daily, MERGEs into core.enriched_orders)
            │
            └──► Step 3: Execute local/Composer dbt models
                         (dbt incremental modeling & test assertions)
```

### 💸 Estimated Cost & Time
*   **Time to complete**: ~1.5 to 2.5 hours.
*   **Estimated GCP Cost**: **$0.00 to $5.00**.
    *   *Free Tier Cover*: Most compute/storage steps fall within GCP's Always Free tiers.
    *   *Cloud Composer Warning*: Composer creates a GKE cluster behind the scenes. Leaving it running will cost ~$2 to $5 per day. **Ensure you perform the teardown steps in Module 8 immediately after completing the lab.**

---

## 🛠️ Module 0: Prerequisites & Initial GCP Project Setup

In this module, you will configure your local workspace, set up your GCP project, enable required APIs, and create a service account with the proper IAM roles.

### 1. Install Local Tools
Ensure you have the following installed on your machine:
*   **Google Cloud SDK (gcloud CLI)**: [Installation Guide](https://cloud.google.com/sdk/docs/install)
*   **Python 3.9+** and `pip`
*   **Git**
*   *Note: Because we use Google Cloud Build, Docker Desktop is NOT required on your local machine.*

### 2. Initialize the gcloud CLI
Open your terminal/command prompt and authenticate with your Google account:
```powershell
# Authenticate your terminal
gcloud auth login

# Set application default credentials (required for local scripts interacting with GCP APIs)
gcloud auth application-default login
```

### 3. Create a New GCP Project
We will create a clean, dedicated GCP project for this lab.

```powershell
# Define your active Project ID
$PROJECT_ID="aws-to-gcp-data-migration"
Write-Output "Project ID: $PROJECT_ID"

# Set your active project context
gcloud config set project $PROJECT_ID
```

> [!WARNING]
> **Quota Mismatch Warning**:
> If you run `gcloud config set project` and receive the warning:
> `WARNING: Your active project does not match the quota project in your local Application Default Credentials file.` or `Cannot add the project to ADC as the quota project...`
> Run the following command to update your local Application Default Credentials (ADC) quota config to match your active project:
> ```powershell
> gcloud auth application-default set-quota-project aws-to-gcp-data-migration
> ```

> [!IMPORTANT]
> **Enable Billing**: Go to the [Google Cloud Console Billing Page](https://console.cloud.google.com/billing) and ensure that your project `aws-to-gcp-data-migration` is linked to an active billing account. Many serverless features (Cloud Run, Dataproc, Composer) will not run without billing enabled.

### 4. Enable GCP APIs
Enable all API services required for our pipeline components:
```powershell
gcloud services enable `
    storage.googleapis.com `
    firestore.googleapis.com `
    run.googleapis.com `
    artifactregistry.googleapis.com `
    eventarc.googleapis.com `
    dataproc.googleapis.com `
    bigquery.googleapis.com `
    composer.googleapis.com `
    compute.googleapis.com `
    logging.googleapis.com
```

### 5. Create a Service Account for the Pipeline
Rather than using admin privileges, we will create a dedicated service account (`retailedge-pipeline-sa`) that runs our ingestion and compute jobs:
```powershell
# Create the service account
gcloud iam service-accounts create retailedge-pipeline-sa `
    --description="Service account for RetailEdge Data Pipeline" `
    --display-name="retailedge-pipeline-sa"

# Assign Dataproc, Storage, BigQuery, and Firestore roles
$SA_EMAIL="retailedge-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com"

# Storage Admin (To read/write GCS)
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/storage.admin"

# BigQuery Admin (To read/write/load BQ tables)
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/bigquery.admin"

# Dataproc Worker (For Serverless Dataproc runs)
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/dataproc.worker"

# Cloud Dataproc Editor (To submit Serverless Dataproc jobs)
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/dataproc.editor"

# Cloud Run Invoker (For Eventarc to trigger Cloud Run)
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/run.invoker"

# Datastore User (To read/write Firestore dedup hashes)
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$SA_EMAIL" --role="roles/datastore.user"
```

---

## 📂 Module 1: Landing Zone Setup (GCS & Firestore)

Here, we will create the core storage layers: GCS buckets for raw/validated data and a Firestore database to act as our deduplication lookup cache.

### 1. Create Google Cloud Storage Buckets
We will configure 4 GCS buckets in our primary region `asia-south1` (Mumbai).
```powershell
# Set default region variable
$REGION="asia-south1"

# Create landing raw bucket (where AWS transfer files land)
gcloud storage buckets create gs://retailedge-landing-raw-$PROJECT_ID --location=$REGION --uniform-bucket-level-access

# Create validated landing bucket (where valid Parquet files are routed)
gcloud storage buckets create gs://retailedge-landing-validated-$PROJECT_ID --location=$REGION --uniform-bucket-level-access

# Create quarantine bucket (where malformed schemas are routed)
gcloud storage buckets create gs://retailedge-landing-quarantine-$PROJECT_ID --location=$REGION --uniform-bucket-level-access

# Create processing staging bucket (for Spark outputs and dependency logs)
gcloud storage buckets create gs://retailedge-processed-$PROJECT_ID --location=$REGION --uniform-bucket-level-access
```

### 2. Create the Firestore Database (Deduplication Store)
Firestore stores the MD5 hashes of incoming files. We initialize it in Native mode.

#### Option A: GCP Console UI (Recommended if CLI lacks permissions)
1. Go to the **Firestore** service page in the GCP Web Console.
2. Click **Create Database**.
3. Choose **Native Mode**.
4. Set Location to `asia-south1` (or your preferred region) and Database ID to `(default)`.
5. Complete the setup wizard (Start in test/production mode).

#### Option B: gcloud CLI
```powershell
# Requires alpha components (gcloud components install alpha) running in admin terminal
gcloud alpha firestore databases create `
    --database="(default)" `
    --location="asia-south1" `
    --type="firestore-native"
```
*Note: A project can only have one default Firestore database, which is why we name it `(default)`.*

---

## 🚀 Module 2: Ingestion Validator Setup (Cloud Run & Eventarc)

In this module, you will containerize the Python validation script (`validate_landing.py`), push it to Artifact Registry, deploy it to Cloud Run, and set up an Eventarc trigger to invoke it whenever a file lands in the `raw` GCS bucket.

### 1. Create an Artifact Registry Repository
We need a Docker repository to store our Cloud Run image:
```powershell
gcloud artifacts repositories create retailedge-docker-repo `
    --repository-format=docker `
    --location=$REGION `
    --description="Docker repository for RetailEdge Cloud Run services"
```

### 2. Prepare the Dockerfile
In your local repository under `02_ingestion/`, make sure you have a `Dockerfile` and a `requirements.txt`.
If you do not have them in `02_ingestion/`, create them:

Create `02_ingestion/requirements.txt`:
```text
functions-framework==3.5.0
pyarrow==14.0.1
google-cloud-storage==2.14.0
google-cloud-firestore==2.14.0
requests==2.31.0
gunicorn==21.2.0
```

Create `02_ingestion/Dockerfile`:
```dockerfile
FROM python:3.9-slim

# Install system dependencies (build-essential for pyarrow/grpc if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY validate_landing.py .

# Use functions-framework to serve the cloud_event function
ENV PORT=8080
EXPOSE 8080

CMD ["functions-framework", "--target", "process_gcs_event", "--signature-type", "cloudevent"]
```

### 3. Build & Push Docker Image (Using Cloud Build)
Run this command from the project root folder. It uploads the source files and builds the container directly in the cloud, removing any need for local Docker:
```powershell
# Build and register the container image directly in GCP Artifact Registry
gcloud builds submit ./02_ingestion `
    --tag "$REGION-docker.pkg.dev/$PROJECT_ID/retailedge-docker-repo/validator:v1"
```

### 4. Deploy the Container to Cloud Run
Deploy the validator. We inject environment variables defining the project ID, target buckets, and service account.
```powershell
gcloud run deploy retailedge-landing-validator `
    --image="$REGION-docker.pkg.dev/$PROJECT_ID/retailedge-docker-repo/validator:v1" `
    --region=$REGION `
    --service-account=$SA_EMAIL `
    --set-env-vars="GCP_PROJECT_ID=$PROJECT_ID,VALIDATED_BUCKET=retailedge-landing-validated-$PROJECT_ID,QUARANTINE_BUCKET=retailedge-landing-quarantine-$PROJECT_ID" `
    --no-allow-unauthenticated
```

### 5. Setup GCS Eventarc Trigger
To trigger Cloud Run whenever a file is created in the `raw` bucket, create an Eventarc trigger.
First, grant the Pub/Sub service agent permission to invoke Eventarc:
```powershell
# Get Project Number
$PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

# Grant pubsub Token Creator role
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:service-$PROJECT_NUMBER@gcp-sa-pubsub.iam.gserviceaccount.com" `
    --role="roles/iam.serviceAccountTokenCreator"

# Grant storage PubSub Publisher role
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:service-$PROJECT_NUMBER@gs-project-accounts.iam.gserviceaccount.com" `
    --role="roles/pubsub.publisher"

# Grant eventarc serviceagent role
gcloud projects add-iam-policy-binding $PROJECT_ID `
    --member="serviceAccount:service-$PROJECT_NUMBER@gcp-sa-eventarc.iam.gserviceaccount.com" `
    --role="roles/eventarc.serviceAgent"
```

Now, create the trigger:
```powershell
gcloud eventarc triggers create retailedge-gcs-trigger `
    --location=$REGION `
    --destination-run-service=retailedge-landing-validator `
    --destination-run-region=$REGION `
    --event-filters="type=google.cloud.storage.object.v1.finalized" `
    --event-filters="bucket=retailedge-landing-raw-$PROJECT_ID" `
    --service-account=$SA_EMAIL
```

---

## ⚡ Module 3: Dataproc Serverless (Spark Job Setup)

Dataproc Serverless runs our PySpark code without requiring us to manage a cluster. To do this, Private Google Access must be enabled on the VPC network.

### 1. Enable Private Google Access on the Subnetwork
Serverless Dataproc batch workers communicate internally over the local GCP network. They require **Private Google Access** to connect to Google APIs (like GCS) without public IP addresses.
```powershell
# Enable Private Google Access on the default network subnet in our region
gcloud compute networks subnets update default `
    --region=$REGION `
    --enable-private-ip-google-access
```

### 2. Stage PySpark Code to GCS
Upload our spark script `process_daily_orders.py` to the processing storage bucket:
```powershell
gcloud storage cp ./03_processing/process_daily_orders.py gs://retailedge-processed-$PROJECT_ID/code/process_daily_orders.py
```

### 3. Test Submit Dataproc Batch Command
Let's verify that Dataproc Serverless can successfully start a Spark session. 
```powershell
# Submit a test batch (this will fail/warn on missing raw data, which is fine! We just want to check the Spark runtime spins up)
gcloud dataproc batches submit pyspark gs://retailedge-processed-$PROJECT_ID/code/process_daily_orders.py `
    --project=$PROJECT_ID `
    --region=$REGION `
    --batch="test-pyspark-batch-001" `
    --service-account=$SA_EMAIL `
    -- `
    --date 2025-06-01 `
    --env prod
```
*Tip: You can monitor batch runs on the [Dataproc Batches Page in Google Cloud Console](https://console.cloud.google.com/dataproc/batches).*

---

## 🗄️ Module 4: BigQuery Warehouse Setup

Here we will create the staging and core datasets, followed by our partition-clustered analytics tables.

### 1. Create BigQuery Datasets
We will create `staging` (for temporary daily data loads) and `core` (for the final production tables).
```powershell
# Create staging dataset
bq --location=$REGION mk --dataset $PROJECT_ID:staging

# Create core warehouse dataset
bq --location=$REGION mk --dataset $PROJECT_ID:core
```

### 2. Create the Production Table with Partitioning and Clustering
Create the production table `core.enriched_orders`. We define the schema and partition the data by `process_date`, clustering it by `user_segment` to minimize search costs.
```powershell
# Create the table schema definition locally or run the BQ DDL directly
bq query --use_legacy_sql=false "
CREATE OR REPLACE TABLE \`$PROJECT_ID.core.enriched_orders\` (
  order_id STRING,
  user_id STRING,
  amount FLOAT64,
  currency STRING,
  process_date DATE,
  user_segment STRING,
  event_count INT64,
  event_types ARRAY<STRING>,
  inserted_at TIMESTAMP,
  updated_at TIMESTAMP
)
PARTITION BY process_date
CLUSTER BY user_segment;
"
```

---

## 🛠️ Module 5: dbt Transformations (Local Config)

We will configure dbt-core to connect to our newly created BigQuery datasets.

### 1. Install dbt-core
Install `dbt-core` and the `dbt-bigquery` adapter locally:
```powershell
pip install dbt-core dbt-bigquery
```

### 2. Configure dbt Profile (`profiles.yml`)
Create a profiles file to authenticate dbt against your GCP project using OAuth (local credentials).
Create/Edit the file `~/.dbt/profiles.yml` (on Windows, `C:\Users\<Your-Username>\.dbt\profiles.yml`):

```yaml
retailedge_dbt:
  outputs:
    prod:
      type: bigquery
      method: oauth
      project: aws-to-gcp-data-migration
      dataset: core
      threads: 4
      timeout_seconds: 300
      location: asia-south1
      priority: interactive
  target: prod
```

### 3. Verify dbt Connection
From inside the `05_transformation/` folder (or where your `dbt_project.yml` is located):
```powershell
# Test connection
dbt debug
```
If this succeeds, dbt has established a secure connection to BigQuery.

---

## 🎼 Module 6: Cloud Composer (Orchestration Setup)

This module deploys Cloud Composer (Apache Airflow) to automate running the pipeline end-to-end.

> [!WARNING]
> Creating a Cloud Composer environment takes **20–30 minutes** to complete. Do not close your terminal or stop execution.

### 1. Create a Cloud Composer 2 Environment
Create a lightweight Composer environment using the **Composer 2 Small** preset:
```powershell
gcloud composer environments create retailedge-composer `
    --location=$REGION `
    --image-version=composer-2.6.0-airflow-2.6.3 `
    --environment-size=small `
    --service-account=$SA_EMAIL
```

### 2. Configure Environment Variables in Airflow
Set required global Airflow variables:
```powershell
gcloud composer environments update retailedge-composer `
    --location=$REGION `
    --update-env-variables="GCP_PROJECT_ID=$PROJECT_ID,VALIDATED_BUCKET=retailedge-landing-validated-$PROJECT_ID,PROCESSED_BUCKET=retailedge-processed-$PROJECT_ID"
```

### 3. Upload DAG and Scripts to Composer GCS Bucket
Cloud Composer creates a dedicated GCS bucket to hold DAG code. Find this bucket and upload your files.
```powershell
# Retrieve the GCS bucket path created for Airflow DAGs
$DAGS_BUCKET=$(gcloud composer environments describe retailedge-composer --location=$REGION --format="value(config.dagGcsPrefix)")
Write-Output "Composer DAGs Bucket: $DAGS_BUCKET"

# Upload the load_and_merge.py utility script to the dags/scripts directory
gcloud storage cp ./04_warehouse/load_and_merge.py "$DAGS_BUCKET/scripts/load_and_merge.py"

# Upload the daily DAG to the Composer dags bucket
gcloud storage cp ./06_orchestration/dags/retailedge_daily_pipeline.py "$DAGS_BUCKET/retailedge_daily_pipeline.py"
```

---

## 🧪 Module 7: End-to-End Execution & Testing

Let's test the entire pipeline by ingesting sample data.

### 1. Copy Mock Data Files Locally
Make sure you have sample Parquet files. If you do not have them, we can generate them or use small mock files.
Let's copy them to the Raw landing zone bucket:
```powershell
# Define the date and upload files mimicking a daily drop
$TEST_DATE="2025-06-01"

# Upload a mock user segment file (dimension) directly to the validated bucket (as segments are dropped weekly/monthly)
# Note: In production this would be copied by a script. We simulate it.
gcloud storage cp ./01_sample_data/segments/segments_2025-05-30.parquet gs://retailedge-landing-validated-$PROJECT_ID/segments/segments_2025-05-30.parquet

# Drop Daily Orders & Events in Raw Bucket
gcloud storage cp ./01_sample_data/orders/orders_2025-06-01.parquet gs://retailedge-landing-raw-$PROJECT_ID/orders/orders_2025-06-01.parquet
gcloud storage cp ./01_sample_data/events/events_2025-06-01.parquet gs://retailedge-landing-raw-$PROJECT_ID/events/events_2025-06-01.parquet
```

### 2. Verify Cloud Run Event-Driven Routing
*   Go to the **Cloud Run Console** and view the logs for `retailedge-landing-validator`.
*   You should see:
    ```text
    New file registered in Firestore. Hash: 8ab2...
    Schema validation PASSED for orders: orders/orders_2025-06-01.parquet
    Promoted to validated: orders/orders_2025-06-01.parquet
    ```
*   Check the GCS bucket `gs://retailedge-landing-validated-$PROJECT_ID/orders/` to ensure `orders_2025-06-01.parquet` was successfully copied there.
*   Check the Firestore Console: under `file_ingestion_registry`, you should see documents named after the MD5 hashes with statuses set to `"validated"`.

### 3. Trigger the Airflow DAG
*   Open the Airflow Web UI from the **Composer Page** in the Google Cloud Console.
*   Find the DAG **`retailedge_daily_pipeline`**.
*   Trigger the DAG manually for execution date **`2025-06-01`**.
*   Watch it execute through the DAG tree views:
    1.  `dataproc_spark_enrichment` (Serverless Spark job joins orders + events + segments)
    2.  `load_staging_to_bq` (Executes python utility inside the DAG to load BQ staging)
    3.  `merge_staging_to_production` (Runs BQ MERGE upsert query)
    4.  `dbt_transformations` (Updates aggregated metrics tables)

### 4. Query BigQuery Production Table
Once the DAG completes, run a check query in the BigQuery Console:
```sql
SELECT process_date, user_segment, count(*) as order_count, sum(amount) as total_revenue, sum(event_count) as total_events
FROM `aws-to-gcp-data-migration.core.enriched_orders`
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

---

## 🧹 Module 8: Cleanup & Teardown (Avoid Charges)

Managed Airflow (Composer) keeps virtual machine nodes running constantly. **To avoid ongoing billing charges, delete the environment as soon as you finish testing.**

Execute the following commands to delete all resources created in this lab:

```powershell
# 1. Delete Cloud Composer Environment
gcloud composer environments delete retailedge-composer --location=$REGION --quiet

# 2. Delete GCS Buckets (and all objects inside)
gcloud storage buckets delete gs://retailedge-landing-raw-$PROJECT_ID --force
gcloud storage buckets delete gs://retailedge-landing-validated-$PROJECT_ID --force
gcloud storage buckets delete gs://retailedge-landing-quarantine-$PROJECT_ID --force
gcloud storage buckets delete gs://retailedge-processed-$PROJECT_ID --force

# 3. Delete Cloud Run Validator
gcloud run services delete retailedge-landing-validator --region=$REGION --quiet

# 4. Delete Eventarc Trigger
gcloud eventarc triggers delete retailedge-gcs-trigger --location=$REGION --quiet

# 5. Delete Artifact Registry Repository
gcloud artifacts repositories delete retailedge-docker-repo --location=$REGION --quiet

# 6. Delete BigQuery Datasets
bq rm -r -f -d $PROJECT_ID:staging
bq rm -r -f -d $PROJECT_ID:core

# 7. Optionally delete the entire GCP Project
gcloud projects delete $PROJECT_ID --quiet
```
