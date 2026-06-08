## Reading Data from BigQuery into Databricks Unity Catalog

There are a few approaches depending on your setup:

---

### Option 1: BigQuery Connector for Spark (Recommended)

Databricks has a native Spark-BigQuery connector.

**Step 1 — Install the connector**

In your cluster libraries, add the Maven coordinate:
```
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:<version>
```
Or via `%pip` in a notebook:
```python
%pip install google-cloud-bigquery-storage
```

**Step 2 — Authenticate**

Either mount a GCP service account key or use Workload Identity. The simplest way is to store the JSON key in Databricks secrets:
```python
spark.conf.set("credentials", dbutils.secrets.get("gcp-scope", "sa-key-json"))
```

**Step 3 — Read from BigQuery**

```python
df = (spark.read
  .format("bigquery")
  .option("project", "your-gcp-project")
  .option("dataset", "your_dataset")
  .option("table", "your_table")
  .option("credentialsFile", "/dbfs/path/to/sa-key.json")
  .load()
)

df.display()
```

**Step 4 — Write to Unity Catalog**

```python
df.write \
  .format("delta") \
  .mode("overwrite") \          # or "append"
  .saveAsTable("catalog.schema.table_name")
```

---

### Option 2: Export BigQuery → GCS → Databricks

Good when the table is very large or cross-cloud network costs matter.

```python
# 1. Export BQ table to GCS (run via BQ or Python client)
from google.cloud import bigquery
client = bigquery.Client()
job = client.extract_table(
    "project.dataset.table",
    "gs://your-bucket/export/*.parquet",
    job_config=bigquery.ExtractJobConfig(destination_format="PARQUET")
)
job.result()

# 2. Read from GCS in Databricks
df = spark.read.parquet("gs://your-bucket/export/")

# 3. Save to Unity Catalog
df.write.format("delta").saveAsTable("catalog.schema.table_name")
```

---

### Option 3: Delta Sharing / Partner Connect

If your org uses **Databricks Partner Connect**, BigQuery can be connected via third-party ETL tools (Fivetran, dbt, Airbyte) that land data directly into Unity Catalog tables. Good for scheduled/incremental loads without writing Spark code.

---

### Option 4: JDBC (small tables only)

Not recommended for large data but works for lookups:
```python
df = (spark.read
  .format("jdbc")
  .option("url", "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443")
  .option("dbtable", "project.dataset.table")
  .option("driver", "com.simba.googlebigquery.jdbc42.Driver")
  .load()
)
```

---

### Unity Catalog tips

- Make sure the target catalog and schema exist first:
  ```sql
  CREATE CATALOG IF NOT EXISTS my_catalog;
  CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;
  ```
- Use `GRANT` statements to set permissions on the ingested table after writing.
- For incremental loads, consider **MERGE INTO** instead of overwrite to avoid full rewrites.

---

**Which approach fits your situation?** The Spark-BigQuery connector (Option 1) is the most direct for ad-hoc or pipeline use, while GCS export (Option 2) is better for very large tables or when minimizing BQ egress costs matters.


### from GCP Service account downloading JSON Key 

<p align="center">
  <img src="images/architecture.png" width="800" alt="Architecture Diagram">
</p>

## Step 1 — Create a Service Account in GCP Console

### 1.1 Open IAM & Admin
- Go to [console.cloud.google.com](https://console.cloud.google.com)
- Select your **project** from the top dropdown
- In the left menu, navigate to **IAM & Admin → Service Accounts**### 1.2 Create New Service Account
- Click **+ Create Service Account** at the top
- Fill in the details:

```
Service account name:  databricks-bq-reader
Service account ID:    databricks-bq-reader          ← auto-filled
Description:           Service account for Databricks to read BigQuery
```
- Click **Create and Continue**

---

## Step 2 — Assign Roles to the Service Account

On the **"Grant this service account access to project"** screen, add these roles one by one by clicking **+ Add Another Role**:

| Role | Purpose |
|---|---|
| `BigQuery Data Viewer` | Read tables and datasets |
| `BigQuery Job User` | Run query jobs (needed for connector) |
| `Storage Object Admin` | Read/write GCS bucket (Option 2 only) |

```
Click: + Add Another Role
Search: "BigQuery Data Viewer" → Select it

Click: + Add Another Role  
Search: "BigQuery Job User" → Select it

Click: + Add Another Role (only if using GCS export)
Search: "Storage Object Admin" → Select it
```

- Click **Continue** → then **Done**

---

## Step 3 — Download the JSON Key

### 3.1 Open the Service Account
- Back on the **Service Accounts** list page
- Click on the service account you just created (`databricks-bq-reader`)### 3.2 Generate the Key
- Click the **Keys** tab at the top
- Click **Add Key → Create New Key**
- Select **JSON** as the key type
- Click **Create**

The JSON file downloads automatically to your machine. It looks like this:

```json
{
  "type": "service_account",
  "project_id": "my-gcp-project",
  "private_key_id": "abc123...",
  "private_key": "-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n",
  "client_email": "databricks-bq-reader@my-gcp-project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
```

> ⚠️ **Keep this file secure** — treat it like a password. Never commit it to Git.

---

## Step 4 — Store the Key in Databricks Secrets

This is the safest way to use the key in notebooks — it never appears in plain text.

### 4.1 Install Databricks CLI (on your local machine)
```bash
pip install databricks-cli

# Configure with your workspace URL and token
databricks configure --token
# Enter: https://your-workspace.azuredatabricks.net
# Enter: your personal access token
```

### 4.2 Create a Secret Scope
```bash
databricks secrets create-scope --scope gcp-scope
```

### 4.3 Store the JSON Key
```bash
# Store the entire JSON file content as a secret
databricks secrets put --scope gcp-scope --key bq-sa-key --string-value "$(cat /path/to/downloaded-key.json)"
```

### 4.4 Verify it's stored
```bash
databricks secrets list --scope gcp-scope
```
Output:
```
Key name      Last updated
----------    ------------
bq-sa-key     1718000000000
```

---

## Step 5 — Use the Key in Databricks Notebook

```python
import json

# Retrieve secret (never prints the actual value)
sa_key_json = dbutils.secrets.get(scope="gcp-scope", key="bq-sa-key")

# Parse to confirm structure (optional check)
sa_info = json.loads(sa_key_json)
print(f"Project: {sa_info['project_id']}")
print(f"Client email: {sa_info['client_email']}")
# Output:
# Project: my-gcp-project
# Client email: databricks-bq-reader@my-gcp-project.iam.gserviceaccount.com
```

Then pass it to Spark as shown in the previous guide:
```python
spark.conf.set("credentials", sa_key_json)
spark.conf.set("spark.datasource.bigquery.credentials", sa_key_json)
```

---

## Summary Checklist

```
✅ Created service account:  databricks-bq-reader
✅ Assigned roles:           BigQuery Data Viewer + BigQuery Job User (+ Storage Object Admin)
✅ Downloaded JSON key:      saved locally as .json file
✅ Uploaded to Databricks:   stored in secret scope "gcp-scope" under key "bq-sa-key"
✅ Verified in notebook:     dbutils.secrets.get() returns valid JSON
```

Once this is done, you're ready to use either Option 1 or Option 2 from the previous guide without any hardcoded credentials.
