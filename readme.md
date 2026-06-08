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
