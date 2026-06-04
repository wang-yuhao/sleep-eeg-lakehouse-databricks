"""Configuration management for Sleep EEG Lakehouse.

Centralizes paths, parameters, and Unity Catalog references.

Exam Coverage:
- Databricks Tooling (20%): Unity Catalog, volumes, schemas
"""

import os
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class CatalogConfig:
    """Unity Catalog configuration."""
    catalog: str = "sleep_eeg_lakehouse"
    schema_bronze: str = "bronze"
    schema_silver: str = "silver"
    schema_gold: str = "gold"
    volume_bronze: str = "sleep_edf_raw"
    
    def get_table_name(self, layer: str, table: str) -> str:
        """Get fully-qualified table name.
        
        Args:
            layer: bronze, silver, or gold
            table: Table name
        
        Returns:
            Fully-qualified table name: catalog.schema.table
        """
        schema_map = {
            "bronze": self.schema_bronze,
            "silver": self.schema_silver,
            "gold": self.schema_gold
        }
        schema = schema_map.get(layer, self.schema_bronze)
        return f"{self.catalog}.{schema}.{table}"
    
    def get_volume_path(self, volume: str = None) -> str:
        """Get Unity Catalog volume path.
        
        Args:
            volume: Volume name (default: volume_bronze)
        
        Returns:
            Volume path: /Volumes/catalog/schema/volume
        """
        vol = volume or self.volume_bronze
        return f"/Volumes/{self.catalog}/{self.schema_bronze}/{vol}"


@dataclass
class PipelineConfig:
    """DLT pipeline configuration."""
    pipeline_name: str = "sleep_eeg_dlt_pipeline"
    target: str = "dev"  # dev, staging, prod
    
    # Auto Loader checkpoint locations
    checkpoint_location: str = "/mcp/bronze_schema_checkpoint"
    
    # Processing parameters
    max_files_per_trigger: int = 10
    trigger_interval: str = "60 seconds"
    
    # Spark configuration
    spark_conf: Dict[str, str] = None
    
    def __post_init__(self):
        if self.spark_conf is None:
            self.spark_conf = {
                "spark.databricks.delta.optimizeWrite.enabled": "true",
                "spark.databricks.delta.autoCompact.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }


@dataclass
class SignalProcessingConfig:
    """EEG signal processing parameters."""
    # Bandpass filter
    lowcut: float = 0.5  # Hz
    highcut: float = 40  # Hz
    
    # Notch filter (power line interference)
    notch_freq: float = 50  # Hz (Europe: 50, US: 60)
    
    # Artifact rejection
    flat_threshold: float = 0.1  # µV
    
    # Sleep event detection
    spindle_freq_range: tuple = (11, 16)  # Hz
    so_freq_range: tuple = (0.5, 1.0)  # Hz
    
    # PAC computation
    pac_n_bins: int = 18  # Phase bins for PAC
    
    # TDA parameters
    tda_embedding_dim: int = 3
    tda_time_delay_ms: float = 100  # milliseconds
    tda_subsample_size: int = 1000  # points


# Global config instances
catalog_config = CatalogConfig()
pipeline_config = PipelineConfig()
signal_config = SignalProcessingConfig()


def load_config_from_env() -> None:
    """Load configuration from environment variables.
    
    Environment variables:
    - DATABRICKS_CATALOG: Unity Catalog name
    - DATABRICKS_TARGET: Deployment target (dev/staging/prod)
    - NOTCH_FREQ: Power line frequency (50 or 60)
    """
    global catalog_config, pipeline_config, signal_config
    
    if os.getenv("DATABRICKS_CATALOG"):
        catalog_config.catalog = os.getenv("DATABRICKS_CATALOG")
    
    if os.getenv("DATABRICKS_TARGET"):
        pipeline_config.target = os.getenv("DATABRICKS_TARGET")
    
    if os.getenv("NOTCH_FREQ"):
        signal_config.notch_freq = float(os.getenv("NOTCH_FREQ"))


# Load config on import
load_config_from_env()