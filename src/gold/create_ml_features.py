"""Gold Layer: Create ML-ready feature table for memory prediction.

This module aggregates features from all Silver/Gold layers:
- Topological features (Betti numbers, persistence)
- Sleep event metrics (spindle density, SO metrics)
- Phase-amplitude coupling (PAC)
- Derived composite features (TMCI)

Exam Coverage:
- Data Processing (30%): Broadcast joins, complex aggregations
- Data Modeling (20%): Wide table design, OPTIMIZE, ZORDER
- Databricks Tooling (20%): Unity Catalog, feature tables
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="gold_ml_features",
    comment="""ML-ready feature table for memory consolidation prediction.
    Combines topological, oscillatory, and coupling features.
    Target variables: spindle_density_per_min, pac_modulation_index""",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.feature.allowColumnDefaults": "supported"
    },
    partition_cols=["subject_id"]
)
@dlt.expect("complete_features", "betti_1 IS NOT NULL AND spindle_count IS NOT NULL", on_violation="warn")
def gold_ml_features():
    """Join all features into ML-ready table.
    
    Returns:
        Wide feature table with:
        - subject_id: Subject identifier
        - TDA features: betti_0, betti_1, betti_2, persistence metrics
        - Sleep events: spindle_count, spindle_density, SO metrics
        - Coupling: pac_modulation_index
        - Composite: tmci (Topological Memory Consolidation Index)
    
    Joins:
    - gold_tda_features (TDA)
    - silver_sleep_spindles (spindles)
    - silver_slow_oscillations (SOs)
    - silver_pac (PAC)
    
    All joins are inner joins on subject_id.
    Databricks AQE will automatically broadcast small tables.
    """
    # Read all feature tables
    tda = dlt.read("gold_tda_features").alias("tda")
    spindles = dlt.read("silver_sleep_spindles").alias("sp")
    sos = dlt.read("silver_slow_oscillations").alias("so")
    pac = dlt.read("silver_pac").alias("pac")
    
    # Join all features
    return (
        tda
            .join(spindles, tda.subject_id == spindles.subject_id, "inner")
            .join(sos, tda.subject_id == sos.subject_id, "inner")
            .join(pac, tda.subject_id == pac.subject_id, "inner")
            .select(
                # Primary key
                F.col("tda.subject_id"),
                
                # Topological features
                F.col("tda.betti_0"),
                F.col("tda.betti_1"),
                F.col("tda.betti_2"),
                F.col("tda.total_persistence_h1"),
                F.col("tda.persistence_entropy"),
                F.col("tda.max_persistence_h1"),
                
                # Spindle features
                F.col("sp.spindle_count"),
                F.col("sp.spindle_density_per_min"),
                F.col("sp.mean_spindle_amplitude"),
                F.col("sp.mean_spindle_frequency"),
                
                # Slow oscillation features
                F.col("so.so_count"),
                F.col("so.so_density_per_min"),
                F.col("so.mean_so_amplitude"),
                
                # Phase-amplitude coupling
                F.col("pac.pac_modulation_index"),
                
                # Composite Topological Memory Consolidation Index (TMCI)
                # TMCI = betti_1 * PAC * spindle_density
                # Higher TMCI = more loops, stronger coupling, more spindles
                (F.col("tda.betti_1") * 
                 F.col("pac.pac_modulation_index") * 
                 F.col("sp.spindle_density_per_min")).alias("tmci"),
                
                # Metadata
                F.current_timestamp().alias("feature_created_time")
            )
    )


# Note: OPTIMIZE and ZORDER are executed post-table creation
# See notebooks/03_analyze_tda_features.py for optimization commands:
# OPTIMIZE sleep_eeg_lakehouse.gold.gold_ml_features
# ZORDER BY (betti_1, spindle_density_per_min, pac_modulation_index)


if __name__ == "__main__":
    pass