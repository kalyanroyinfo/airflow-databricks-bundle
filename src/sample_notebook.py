# Databricks notebook source
# =============================================================================
# sample_notebook.py — Sample ETL Notebook
# =============================================================================
# Demonstrates a simple ETL pipeline deployed via Databricks Asset Bundles
# and triggered by Apache Airflow.
#
# Parameters (passed from Airflow via notebook_params):
#   env      — Target environment (dev/prod). Default: "dev"
#   run_date — Execution date in YYYY-MM-DD format. Default: "2026-01-01"
#
# Steps:
#   1. Create sample data
#   2. Transform data (add tax, timestamp, environment)
#   3. Print summary and exit with SUCCESS
# =============================================================================
# MAGIC %md
# MAGIC # Sample ETL Notebook
# MAGIC This notebook is deployed via **Databricks Asset Bundles** and triggered by **Apache Airflow**.

# COMMAND ----------

# Read parameters passed from Airflow / DAB
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("run_date", "2026-01-01", "Run Date")

env = dbutils.widgets.get("env")
run_date = dbutils.widgets.get("run_date")

print(f"Running ETL for environment: {env}, date: {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Data

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(id=1, name="Alice", amount=100.0, date=run_date),
    Row(id=2, name="Bob", amount=200.0, date=run_date),
    Row(id=3, name="Charlie", amount=150.0, date=run_date),
]

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transform Data

# COMMAND ----------

from pyspark.sql import functions as F

df_transformed = (
    df
    .withColumn("amount_with_tax", F.col("amount") * 1.18)
    .withColumn("processed_at", F.current_timestamp())
    .withColumn("environment", F.lit(env))
)

df_transformed.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Summary

# COMMAND ----------

record_count = df_transformed.count()
total_amount = df_transformed.agg(F.sum("amount_with_tax")).collect()[0][0]

print(f"✅ ETL Complete!")
print(f"   Records processed: {record_count}")
print(f"   Total amount (with tax): {total_amount:.2f}")
print(f"   Environment: {env}")
print(f"   Run date: {run_date}")

# COMMAND ----------

# Return success
dbutils.notebook.exit("SUCCESS")
