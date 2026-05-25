# Hands-On Lab: Creating the Eventarc Trigger

Since you have your GCP account open, let's build the exact trigger we've been talking about. We will configure Google Cloud Storage to fire an event to a Cloud Run service the moment a file is uploaded.

---

## Step 1: Enable the Required APIs
Before you do anything, you must tell GCP you want to use these services.
1. In the top search bar, search for **Eventarc API** and click **Enable**.
2. Search for **Cloud Run API** and click **Enable**.
3. Search for **Cloud Pub/Sub API** and click **Enable** (Eventarc uses Pub/Sub under the hood to route the messages).

---

## Step 2: Deploy a "Dummy" Cloud Run Service
To create a trigger, you need a destination. Let's deploy Google's default "hello" container just to prove the trigger works.

1. Go to **Cloud Run** in the GCP console.
2. Click **+ Create Service**.
3. Select **Test with a sample container**.
4. Service Name: `validate-landing-service`
5. Region: `us-central1` (or whatever is closest to you).
6. Authentication: Select **Require authentication** (This is crucial! We don't want the public internet triggering our pipeline, only Eventarc).
7. Click **Create**.

---

## Step 3: Create the GCS Bucket
1. Go to **Cloud Storage (GCS)**.
2. Click **+ Create Bucket**.
3. Name: `retailedge-raw-prod-123` (Buckets must be globally unique, so add some random numbers at the end).
4. Region: `us-central1` (Keep it in the same region as your Cloud Run service).
5. Click **Create**.

---

## Step 4: Create the Eventarc Trigger (The Magic Step)

### Method A: Using the GCP Console (UI)
1. Go to **Eventarc** in the GCP console.
2. Click **+ Create Trigger**.
3. **Trigger Name:** `trigger-raw-to-validator`
4. **Event Provider:** Select `Cloud Storage`.
5. **Event Type:** Select `google.cloud.storage.object.v1.finalized` (This means "when a file upload completes").
6. **Bucket:** Select the bucket you created in Step 3.
7. **Destination:**
   * Select **Cloud Run**.
   * Select your `validate-landing-service`.
8. **Service Account:** Select the `Compute Engine default service account`. (In production, you'd create a specific one, but this is fine for the lab).
9. Click **Create**. *(Note: It might take 1-2 minutes for the trigger to fully activate).*

---

### Method B: The Interview Answer (Using `gcloud` CLI)
*If the interviewer asks how to do this programmatically, this is the command you tell them:*

```bash
gcloud eventarc triggers create trigger-raw-to-validator \
    --location=us-central1 \
    --destination-run-service=validate-landing-service \
    --destination-run-region=us-central1 \
    --event-filters="type=google.cloud.storage.object.v1.finalized" \
    --event-filters="bucket=retailedge-raw-prod-123" \
    --service-account=123456789-compute@developer.gserviceaccount.com
```

---

## Step 5: Test It!

Let's prove it works.

1. Go to your **Cloud Storage** bucket (`retailedge-raw-prod-123`).
2. Click **Upload Files** and upload any random text file or CSV from your computer.
3. As soon as the upload finishes, immediately go to **Cloud Run**.
4. Click on your `validate-landing-service`.
5. Click on the **Logs** tab.
6. You will see a brand new log entry! If you expand the log payload, you will see a JSON object containing the exact name of the file you just uploaded. 

**Congratulations!** You just built an enterprise-grade, event-driven ingestion trigger.
