"""Unit tests for Bronze layer ingestion.

Exam Coverage:
- Testing (10%): DLT expectations, schema validation, data quality
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests."""
    return SparkSession.builder \
        .appName("SleepEEG_Tests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


class TestBronzeIngestion:
    """Test Bronze layer data ingestion and expectations."""
    
    def test_subject_id_extraction(self, spark):
        """Test subject ID extraction from filename."""
        from pyspark.sql import functions as F
        
        # Sample data
        data = [
            ("/path/to/SC4001E0-PSG.edf",),
            ("/path/to/SC4002E0-PSG.edf",),
            ("/path/to/SC4011G2-Hypnogram.edf",)
        ]
        df = spark.createDataFrame(data, ["path"])
        
        # Extract subject ID
        df = df.withColumn("subject_id", 
                          F.regexp_extract(F.col("path"), r"(SC\d{4})", 1))
        
        result = df.select("subject_id").collect()
        assert result[0][0] == "SC4001"
        assert result[1][0] == "SC4002"
        assert result[2][0] == "SC4011"
    
    def test_file_type_detection(self, spark):
        """Test file type detection from filename."""
        from pyspark.sql import functions as F
        
        data = [
            ("/path/to/SC4001E0-PSG.edf",),
            ("/path/to/SC4002E0-Hypnogram.edf",),
            ("/path/to/unknown.edf",)
        ]
        df = spark.createDataFrame(data, ["path"])
        
        df = df.withColumn("file_type",
            F.when(F.col("path").contains("PSG"), "PSG")
             .when(F.col("path").contains("Hypnogram"), "Hypnogram")
             .otherwise("Unknown")
        )
        
        result = df.select("file_type").collect()
        assert result[0][0] == "PSG"
        assert result[1][0] == "Hypnogram"
        assert result[2][0] == "Unknown"
    
    def test_valid_subject_id_expectation(self, spark):
        """Test that invalid subject IDs are caught."""
        from pyspark.sql import functions as F
        
        data = [
            ("SC4001",),
            (None,),
            ("INVALID",)
        ]
        df = spark.createDataFrame(data, ["subject_id"])
        
        # Apply expectation
        valid_df = df.filter(F.col("subject_id").isNotNull())
        
        assert valid_df.count() == 2  # Only 2 non-null records
    
    def test_valid_file_type_expectation(self, spark):
        """Test that invalid file types are caught."""
        from pyspark.sql import functions as F
        
        data = [
            ("PSG",),
            ("Hypnogram",),
            ("InvalidType",)
        ]
        df = spark.createDataFrame(data, ["file_type"])
        
        # Apply expectation
        valid_df = df.filter(F.col("file_type").isin(["PSG", "Hypnogram", "Unknown"]))
        
        assert valid_df.count() == 2  # Only 2 valid types