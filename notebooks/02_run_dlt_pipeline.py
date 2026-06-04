# Databricks notebook source
# MAGIC %md
# MAGIC # Run Delta Live Tables Pipeline
# MAGIC 
# MAGIC This notebook demonstrates how to:
# MAGIC 1. Deploy DLT pipeline using Databricks Asset Bundles
# MAGIC 2. Monitor pipeline execution
# MAGIC 3. View event logs and data quality expectations
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog setup complete (run `01_setup_unity_catalog`)
# MAGIC - EDF files uploaded to `/Volumes/sleep_eeg_lakehouse/bronze/sleep_edf_raw/`
# MAGIC - Databricks CLI installed and authenticated
# MAGIC 
# MAGIC **Exam Coverage:**
# MAGIC - Data Processing (30%): DLT orchestration, incremental processing
# MAGIC - Databricks Tooling (20%): Asset Bundles, CLI, event logs
# MAGIC - Monitoring (10%): DLT expectations, data quality metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Pipeline with Asset Bundles
# MAGIC 
# MAGIC Run from terminal:
# MAGIC ```bash
# MAGIC # From repo root
# MAGIC databricks bundle deploy --target dev
# MAGIC 
# MAGIC # Start pipeline
# MAGIC databricks bundle run sleep_eeg_dlt_pipeline --target dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitor Pipeline via API

# COMMAND ----------

import requests
import os

# Get Databricks workspace URL and token
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# DLT pipeline ID (replace with your pipeline ID after deployment)
pipeline_id = "<PIPELINE_ID_FROM_BUNDLE_DEPLOY>"

# Get pipeline status
response = requests.get(
    f"https://{workspace_url}/api/2.0/pipelines/{pipeline_id}",
    headers={"Authorization": f"Bearer {token}"}
)

if response.status_code == 200:
    pipeline_info = response.json()
    print(f"Pipeline: {pipeline_info['name']}")
    print(f"State: {pipeline_info.get('state', 'UNKNOWN')}")
    print(f"Health: {pipeline_info.get('health', 'UNKNOWN')}")
else:
    print(f"Error: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Data Quality Metrics

# COMMAND ----------

# Query event log for expectations
spark.sql("""
SELECT 
    timestamp,
    details:flow_progress.data_quality.expectations.name as expectation_name,
    details:flow_progress.data_quality.expectations.passed_records,
    details:flow_progress.data_quality.expectations.failed_records
FROM event_log(TABLE(bronze.bronze_sleep_edf_raw))
WHERE details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Layer

# COMMAND ----------

# Check ingested files
df_bronze = spark.table("bronze.bronze_sleep_edf_raw")
print(f"Total records: {df_bronze.count()}")

display(
    df_bronze
    .groupBy("file_type", "subject_id")
    .count()
    .orderBy("subject_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Layer

# COMMAND ----------

# Check preprocessed EEG
display(
    spark.table("silver.silver_eeg_preprocessed")
    .select("subject_id", "n_channels", "sampling_rate", "duration_sec")
)

# Check detected spindles
display(
    spark.table("silver.silver_sleep_spindles")
    .select("subject_id", "spindle_count", "spindle_density_per_min")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Layer

# COMMAND ----------

# Check TDA features
display(
    spark.table("gold.gold_tda_features")
    .select("subject_id", "betti_1", "betti_2", "persistence_entropy")
)

# Check ML features
display(
    spark.table("gold.gold_ml_features")
    .select("subject_id", "betti_1", "spindle_density_per_min", "pac_modulation_index", "tmci")
)

# COMMAND ----------

print("✅ DLT pipeline verification complete!")
print("Next: Analyze TDA features (notebook 03)")