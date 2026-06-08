## Option 1: Spark-BigQuery Connector — Detailed Walkthrough

### Prerequisites
- Databricks Runtime 11.x+ (with Unity Catalog enabled)
- GCP Service Account with roles: `BigQuery Data Viewer`, `BigQuery Job User`
- For Option 2 additionally: `Storage Object Admin` on the GCS bucket

---

### Step 1 — Create & Store GCP Service Account Key

In GCP Console, create a service account and download the JSON key. Then store it in Databricks:

```bash
# Upload key to DBFS (do this once from local machine or CI/CD)
databricks fs cp sa-key.json dbfs:/secrets/gcp/sa-key.json
```

Better practice — store in Databricks Secret Scope:
```bash
# Create scope
databricks secrets create-scope --scope gcp-scope

# Store the entire JSON key as a secret
databricks secrets put --scope gcp-scope --key bq-sa-key
```

---

### Step 2 — Install Spark-BigQuery Connector on Cluster

Go to **Cluster → Libraries → Install New → Maven** and add:
```
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1
```

Or install at notebook level (restarts needed on some runtimes):
```python
%pip install google-cloud-bigquery google-cloud-bigquery-storage
```

---

### Step 3 — Configure Spark Session

```python
# In your Databricks notebook

# Option A: Use service account JSON stored in DBFS
spark.conf.set(
    "spark.hadoop.google.cloud.auth.service.account.enable", "true"
)
spark.conf.set(
    "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
    "/dbfs/secrets/gcp/sa-key.json"
)

# Option B: Use key stored in Databricks Secrets (more secure)
import json
sa_key = dbutils.secrets.get(scope="gcp-scope", key="bq-sa-key")

spark.conf.set("credentials", sa_key)
spark.conf.set(
    "spark.datasource.bigquery.credentials",
    sa_key
)
```

---

### Step 4 — Read from BigQuery

```python
# --- Basic read ---
df = (
    spark.read
    .format("bigquery")
    .option("project", "my-gcp-project")
    .option("parentProject", "my-gcp-project")   # for billing
    .option("table", "my_dataset.my_table")       # dataset.table format
    .load()
)

df.printSchema()
df.display()
```

```python
# --- Read with SQL filter (pushdown to BQ — avoids full scan) ---
df_filtered = (
    spark.read
    .format("bigquery")
    .option("project", "my-gcp-project")
    .option("parentProject", "my-gcp-project")
    .option("table", "my_dataset.orders")
    .option("filter", "order_date >= '2024-01-01' AND status = 'COMPLETED'")
    .load()
)

df_filtered.display()
```

```python
# --- Read using a BigQuery SQL query instead of table ---
query = """
    SELECT
        customer_id,
        SUM(order_value) AS total_spend,
        COUNT(*) AS order_count
    FROM `my-gcp-project.my_dataset.orders`
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
"""

df_query = (
    spark.read
    .format("bigquery")
    .option("project", "my-gcp-project")
    .option("query", query)
    .load()
)

df_query.display()
```

---

### Step 5 — Write to Unity Catalog

```python
# Ensure target catalog and schema exist
spark.sql("CREATE CATALOG IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.sales")

# --- Full load (overwrite) ---
(
    df_filtered
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")     # allows schema evolution
    .saveAsTable("silver.sales.orders")
)

print("Table written successfully.")
spark.sql("SELECT COUNT(*) FROM silver.sales.orders").display()
```

```python
# --- Incremental load using MERGE (upsert) ---
from delta.tables import DeltaTable

# Write incoming data to a temp view
df_filtered.createOrReplaceTempView("bq_orders_incoming")

# Check if target table exists
if spark.catalog.tableExists("silver.sales.orders"):
    target = DeltaTable.forName(spark, "silver.sales.orders")
    
    target.alias("target").merge(
        df_filtered.alias("source"),
        "target.order_id = source.order_id"   # join key
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

    print("Merge complete.")
else:
    # First run — create the table
    df_filtered.write.format("delta").saveAsTable("silver.sales.orders")
    print("Table created.")
```

---
---

## Option 2: BigQuery → GCS → Databricks — Detailed Walkthrough

This is better for **very large tables** (hundreds of GBs) since it avoids Spark-BQ connector overhead and cross-cloud streaming costs.

---

### Step 1 — Export BigQuery Table to GCS

**Method A: From BigQuery Console**
- Open BQ table → Export → Export to GCS
- Choose format: Parquet (recommended) or CSV
- Set destination: `gs://your-bucket/exports/orders/`

**Method B: Python BigQuery Client (run in Databricks notebook)**

```python
%pip install google-cloud-bigquery google-cloud-storage

from google.cloud import bigquery
import json

# Authenticate using service account key
sa_key_json = dbutils.secrets.get(scope="gcp-scope", key="bq-sa-key")
sa_info = json.loads(sa_key_json)

from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_info(sa_info)

client = bigquery.Client(project="my-gcp-project", credentials=credentials)

# Configure export job
destination_uri = "gs://my-bucket/exports/orders/*.parquet"

job_config = bigquery.ExtractJobConfig(
    destination_format=bigquery.DestinationFormat.PARQUET,
    compression=bigquery.Compression.SNAPPY,     # smaller files
)

extract_job = client.extract_table(
    source="my-gcp-project.my_dataset.orders",
    destination_uris=destination_uri,
    job_config=job_config,
)

# Wait for job to complete
result = extract_job.result()
print(f"Export done. Exported {result.destination_uri_file_counts} file(s).")
```

**Method C: BigQuery SQL Export (run directly in BQ)**
```sql
EXPORT DATA
  OPTIONS (
    uri = 'gs://my-bucket/exports/orders/*.parquet',
    format = 'PARQUET',
    compression = 'SNAPPY',
    overwrite = true
  )
AS (
  SELECT *
  FROM `my-gcp-project.my_dataset.orders`
  WHERE order_date >= '2024-01-01'
);
```

---

### Step 2 — Mount GCS Bucket in Databricks (one-time setup)

```python
# Mount GCS bucket to DBFS
sa_key_json = dbutils.secrets.get(scope="gcp-scope", key="bq-sa-key")

dbutils.fs.mount(
    source="gs://my-bucket",
    mount_point="/mnt/gcs-my-bucket",
    extra_configs={
        "google.cloud.auth.service.account.enable": "true",
        "fs.gs.auth.service.account.json.keyfile.content": sa_key_json
    }
)

# Verify mount
dbutils.fs.ls("/mnt/gcs-my-bucket/exports/orders/")
```

Or use direct GCS path without mounting (Unity Catalog preferred approach):
```python
# Configure GCS access via Spark config
spark.conf.set(
    "fs.gs.auth.service.account.json.keyfile.content",
    dbutils.secrets.get(scope="gcp-scope", key="bq-sa-key")
)
```

---

### Step 3 — Read Exported Files into Spark

```python
# --- Read Parquet from GCS (via mount) ---
df = spark.read.parquet("/mnt/gcs-my-bucket/exports/orders/")

# --- Or directly via GCS URI ---
df = spark.read.parquet("gs://my-bucket/exports/orders/")

df.printSchema()
print(f"Row count: {df.count():,}")
df.display()
```

```python
# --- Read CSV export (if Parquet wasn't used) ---
df_csv = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/gcs-my-bucket/exports/orders/")
)
```

```python
# --- Add ingestion metadata columns (good practice) ---
from pyspark.sql import functions as F

df_enriched = df.withColumn(
    "ingestion_timestamp", F.current_timestamp()
).withColumn(
    "source_system", F.lit("bigquery")
)

df_enriched.display()
```

---

### Step 4 — Write to Unity Catalog with Partitioning

```python
# Ensure target exists
spark.sql("CREATE CATALOG IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver.sales")

# --- Write with partitioning for large tables ---
(
    df_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_date")              # partition on date for fast queries
    .saveAsTable("silver.sales.orders")
)

# Verify
spark.sql("""
    SELECT order_date, COUNT(*) as row_count
    FROM silver.sales.orders
    GROUP BY order_date
    ORDER BY order_date DESC
    LIMIT 10
""").display()
```

```python
# --- Optimize the Delta table after load (improves query perf) ---
spark.sql("OPTIMIZE silver.sales.orders ZORDER BY (customer_id)")
spark.sql("ANALYZE TABLE silver.sales.orders COMPUTE STATISTICS")
```

---

### Step 5 — Clean Up GCS Export Files (optional)

```python
from google.cloud import storage

storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("my-bucket")

blobs = bucket.list_blobs(prefix="exports/orders/")
for blob in blobs:
    blob.delete()

print("GCS export files cleaned up.")
```

---

## Quick Comparison

| | Option 1 (Spark-BQ Connector) | Option 2 (GCS Export) |
|---|---|---|
| **Best for** | Small–medium tables, ad-hoc | Very large tables, scheduled ETL |
| **Setup complexity** | Low | Medium |
| **Speed** | Moderate (streaming read) | Fast (bulk Parquet read) |
| **Cost** | BQ query + egress | BQ export + GCS storage + egress |
| **Schema control** | Auto from BQ | Auto from Parquet |
| **Incremental load** | Easy with filter pushdown | Needs partition logic |
