"""Bronze Layer: Ingest raw Sleep-EDF files using Auto Loader.

This module implements the Bronze layer of the medallion architecture,
ingesting raw EDF (European Data Format) polysomnography files from
PhysioNet Sleep-EDF Expanded dataset using Databricks Auto Loader.

Key Features:
- Incremental ingestion with cloudFiles (Auto Loader)
- Schema inference and evolution
- Data quality expectations (DLT)
- Metadata extraction (subject ID, file type)

References:
- Kemp et al. (2000). Analysis of a sleep-dependent neuronal feedback loop.
  IEEE Trans Biomed Eng, 47(9), 1185-1194. DOI: 10.1109/10.867928
- PhysioNet Sleep-EDF Expanded: https://physionet.org/content/sleep-edfx/1.0.0/

Exam Coverage:
- Data Processing (30%): Structured Streaming with Auto Loader
- Data Modeling (20%): Delta Lake tables with schema management
- Monitoring (10%): DLT expectations for data quality
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, TimestampType


@dlt.table(
    name="bronze_sleep_edf_raw",
    comment="""Raw EDF files from PhysioNet Sleep-EDF Expanded dataset.
    Contains PSG (polysomnography) and Hypnogram (sleep staging) files.
    Binary content will be parsed in Silver layer using MNE-Python.""",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name"
    },
    partition_cols=["file_type", "subject_id"]
)
@dlt.expect_or_drop("valid_subject_id", "subject_id IS NOT NULL")
@dlt.expect_or_drop("valid_file_type", "file_type IN ('PSG', 'Hypnogram', 'Unknown')")
@dlt.expect_or_fail("non_empty_content", "length(content) > 0")
def bronze_sleep_edf_raw():
    """Ingest raw EDF files using Auto Loader.
    
    Returns:
        DataFrame with columns:
        - path: Full path to the EDF file
        - content: Binary content of the EDF file
        - modificationTime: File modification timestamp
        - ingestion_time: Timestamp when file was ingested
        - subject_id: Extracted subject identifier (SC0000N format)
        - file_type: PSG (EEG recording) or Hypnogram (sleep stages)
    
    Data Quality Expectations:
    - valid_subject_id: Subject ID must not be null
    - valid_file_type: File type must be PSG, Hypnogram, or Unknown
    - non_empty_content: File content cannot be empty
    """
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "binaryFile")  # Read as binary
            .option("cloudFiles.schemaLocation", "/mcp/bronze_schema_checkpoint")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("pathGlobFilter", "*.edf")  # Only EDF files
            .option("recursiveFileLookup", "true")  # Search subdirectories
            # Auto Loader checkpoint for incremental processing
            .option("cloudFiles.useNotifications", "false")  # Directory listing mode
            .load("/Volumes/sleep_eeg_lakehouse/bronze/sleep_edf_raw")
            # Add metadata columns
            .withColumn("ingestion_time", F.current_timestamp())
            # Extract subject ID from filename (pattern: SC4001E0-PSG.edf -> SC4001)
            .withColumn("subject_id", F.regexp_extract(F.col("path"), r"(SC\d{4})", 1))
            # Determine file type from filename
            .withColumn("file_type",
                F.when(F.col("path").contains("PSG"), "PSG")
                 .when(F.col("path").contains("Hypnogram"), "Hypnogram")
                 .otherwise("Unknown")
            )
            # Add file size for monitoring
            .withColumn("file_size_mb", F.round(F.length(F.col("content")) / (1024.0 * 1024.0), 2))
    )


@dlt.table(
    name="bronze_edf_metadata",
    comment="""Metadata catalog for ingested EDF files.
    Aggregated statistics for monitoring and data quality tracking.""",
    table_properties={"quality": "bronze"}
)
def bronze_edf_metadata():
    """Aggregate metadata statistics for monitoring.
    
    Returns:
        DataFrame with aggregated statistics:
        - subject_id: Subject identifier
        - psg_file_count: Number of PSG files for this subject
        - hypnogram_file_count: Number of Hypnogram files
        - total_size_mb: Total file size in MB
        - latest_ingestion: Most recent ingestion timestamp
    """
    return (
        dlt.read("bronze_sleep_edf_raw")
            .groupBy("subject_id")
            .agg(
                F.sum(F.when(F.col("file_type") == "PSG", 1).otherwise(0)).alias("psg_file_count"),
                F.sum(F.when(F.col("file_type") == "Hypnogram", 1).otherwise(0)).alias("hypnogram_file_count"),
                F.sum("file_size_mb").alias("total_size_mb"),
                F.max("ingestion_time").alias("latest_ingestion")
            )
            # Data quality: Each subject should have PSG and Hypnogram
            .withColumn("is_complete",
                F.when((F.col("psg_file_count") >= 1) & (F.col("hypnogram_file_count") >= 1), True)
                 .otherwise(False)
            )
    )


if __name__ == "__main__":
    # This module is designed to run as part of a DLT pipeline
    # For local testing, use:
    # databricks bundle deploy --target dev
    # databricks bundle run sleep_eeg_dlt_pipeline --target dev
    pass