# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog for Sleep EEG Lakehouse
# MAGIC 
# MAGIC This notebook creates the Unity Catalog structure:
# MAGIC - Catalog: `sleep_eeg_lakehouse`
# MAGIC - Schemas: `bronze`, `silver`, `gold`
# MAGIC - Volume: `sleep_edf_raw` (Bronze layer)
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog metastore must be enabled
# MAGIC - User must have `CREATE CATALOG` privilege
# MAGIC 
# MAGIC **Exam Coverage:**
# MAGIC - Databricks Tooling (20%): Unity Catalog, volumes, external locations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

# Create catalog if not exists
spark.sql("""
CREATE CATALOG IF NOT EXISTS sleep_eeg_lakehouse
COMMENT 'Sleep EEG data lakehouse for memory consolidation research'
""")

# Set as current catalog
spark.sql("USE CATALOG sleep_eeg_lakehouse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas (Bronze, Silver, Gold)

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw Sleep-EDF files ingested via Auto Loader'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Preprocessed EEG signals, sleep events, and PAC metrics'
""")

spark.sql("""
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'TDA features and ML-ready aggregated features'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for Raw EDF Files

# COMMAND ----------

# Create managed volume in Bronze schema
spark.sql("""
CREATE VOLUME IF NOT EXISTS bronze.sleep_edf_raw
COMMENT 'Storage for raw Sleep-EDF files from PhysioNet'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# Show all schemas
display(spark.sql("SHOW SCHEMAS IN sleep_eeg_lakehouse"))

# COMMAND ----------

# Show volume
display(spark.sql("SHOW VOLUMES IN bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Sample Data (Optional)
# MAGIC 
# MAGIC Download Sleep-EDF Expanded dataset from PhysioNet:
# MAGIC ```bash
# MAGIC wget -r -np -nH --cut-dirs=3 https://physionet.org/files/sleep-edfx/1.0.0/
# MAGIC ```
# MAGIC 
# MAGIC Upload to volume:
# MAGIC ```python
# MAGIC dbutils.fs.cp("file:/local/path/sleep-edfx/", 
# MAGIC              "/Volumes/sleep_eeg_lakehouse/bronze/sleep_edf_raw/", 
# MAGIC              recurse=True)
# MAGIC ```

# COMMAND ----------

print("✅ Unity Catalog setup complete!")
print("Next: Run DLT pipeline using databricks bundle deploy")