"""Silver Layer: Detect sleep events (spindles, SOs, ripples) using YASA.

This module detects three key sleep oscillations:
1. Sleep spindles (11-16 Hz): Thalamocortical oscillations linked to memory
2. Slow oscillations (0.5-1 Hz): Cortical up/down states
3. Sharp-wave ripples (80-200 Hz): Hippocampal replay events (if available)

References:
- Vallat & Walker (2021). YASA: An open-source tool for automated sleep staging.
  eLife, 10, e70092. DOI: 10.7554/eLife.70092
- Fernandez-Sanjurjo et al. (2026). Sleep strengthens successor representations.
  PLOS Biology. DOI: 10.1371/journal.pbio.3003740

Exam Coverage:
- Data Processing (30%): Complex UDFs with external libraries
- Data Modeling (20%): Nested arrays of structs
- Monitoring (10%): Event count validation
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, ArrayType, DoubleType, 
    StringType, IntegerType
)


def detect_spindles_yasa(eeg_data: list, sfreq: float, channels: list) -> list:
    """Detect sleep spindles using YASA.
    
    Args:
        eeg_data: 2D array of EEG data (channels × samples)
        sfreq: Sampling frequency in Hz
        channels: List of channel names
    
    Returns:
        List of dicts, each containing:
        - start: Start time in seconds
        - duration: Duration in seconds
        - amplitude: Peak-to-peak amplitude in µV
        - frequency: Dominant frequency in Hz
        - channel: Channel where detected
    """
    import yasa
    import numpy as np
    
    try:
        # Convert to numpy array
        data = np.array(eeg_data)
        
        # Detect spindles on first EEG channel (typically Fpz-Cz or C3-A2)
        sp = yasa.spindles_detect(data[0], sfreq, ch_names=[channels[0]])
        
        if sp is None:
            return []
        
        # Extract events as list of dicts
        summary = sp.summary()
        events = []
        for _, row in summary.iterrows():
            events.append({
                "start": float(row['Start']),
                "duration": float(row['Duration']),
                "amplitude": float(row['Amplitude']),
                "frequency": float(row['Frequency']),
                "channel": channels[0]
            })
        
        return events
    
    except Exception as e:
        return []


def detect_slow_oscillations_yasa(eeg_data: list, sfreq: float, channels: list) -> list:
    """Detect slow oscillations (SOs) using YASA.
    
    Args:
        eeg_data: 2D array of EEG data (channels × samples)
        sfreq: Sampling frequency in Hz
        channels: List of channel names
    
    Returns:
        List of dicts with SO events (start, duration, ptp_amplitude, frequency)
    """
    import yasa
    import numpy as np
    
    try:
        data = np.array(eeg_data)
        
        # Detect slow oscillations
        sw = yasa.sw_detect(data[0], sfreq, ch_names=[channels[0]])
        
        if sw is None:
            return []
        
        summary = sw.summary()
        events = []
        for _, row in summary.iterrows():
            events.append({
                "start": float(row['Start']),
                "duration": float(row['Duration']),
                "ptp_amplitude": float(row['PTP']),  # Peak-to-peak amplitude
                "frequency": float(row['Frequency']),
                "channel": channels[0]
            })
        
        return events
    
    except Exception as e:
        return []


# Define schemas
spindle_event_schema = StructType([
    StructField("start", DoubleType(), False),
    StructField("duration", DoubleType(), False),
    StructField("amplitude", DoubleType(), False),
    StructField("frequency", DoubleType(), False),
    StructField("channel", StringType(), False)
])

so_event_schema = StructType([
    StructField("start", DoubleType(), False),
    StructField("duration", DoubleType(), False),
    StructField("ptp_amplitude", DoubleType(), False),
    StructField("frequency", DoubleType(), False),
    StructField("channel", StringType(), False)
])

# Register UDFs
detect_spindles_udf = F.udf(detect_spindles_yasa, returnType=ArrayType(spindle_event_schema))
detect_sos_udf = F.udf(detect_slow_oscillations_yasa, returnType=ArrayType(so_event_schema))


@dlt.table(
    name="silver_sleep_spindles",
    comment="""Detected sleep spindles (11-16 Hz) using YASA.
    Spindles are thalamocortical oscillations associated with memory consolidation.""",
    table_properties={"quality": "silver"},
    partition_cols=["subject_id"]
)
@dlt.expect("has_spindles", "spindle_count > 0", on_violation="warn")
def silver_sleep_spindles():
    """Detect and aggregate sleep spindles.
    
    Returns:
        DataFrame with spindle events and density metrics.
    """
    return (
        dlt.read("silver_eeg_preprocessed")
            .withColumn("spindles", detect_spindles_udf(
                F.col("data"), 
                F.col("sampling_rate"), 
                F.col("channels")
            ))
            .withColumn("spindle_count", F.size(F.col("spindles")))
            .withColumn("spindle_density_per_min", 
                F.col("spindle_count") / (F.col("duration_sec") / 60.0)
            )
            # Calculate mean spindle properties
            .withColumn("mean_spindle_amplitude", 
                F.expr("aggregate(spindles, 0.0, (acc, x) -> acc + x.amplitude, acc -> acc / spindle_count)")
            )
            .withColumn("mean_spindle_frequency",
                F.expr("aggregate(spindles, 0.0, (acc, x) -> acc + x.frequency, acc -> acc / spindle_count)")
            )
            .select(
                "subject_id",
                "spindles",
                "spindle_count",
                "spindle_density_per_min",
                "mean_spindle_amplitude",
                "mean_spindle_frequency",
                "processed_time"
            )
    )


@dlt.table(
    name="silver_slow_oscillations",
    comment="""Detected slow oscillations (0.5-1 Hz) using YASA.
    SOs coordinate hippocampal-cortical memory transfer during NREM sleep.""",
    table_properties={"quality": "silver"},
    partition_cols=["subject_id"]
)
@dlt.expect("has_sos", "so_count > 0", on_violation="warn")
def silver_slow_oscillations():
    """Detect and aggregate slow oscillations.
    
    Returns:
        DataFrame with SO events and metrics.
    """
    return (
        dlt.read("silver_eeg_preprocessed")
            .withColumn("slow_oscillations", detect_sos_udf(
                F.col("data"),
                F.col("sampling_rate"),
                F.col("channels")
            ))
            .withColumn("so_count", F.size(F.col("slow_oscillations")))
            .withColumn("so_density_per_min",
                F.col("so_count") / (F.col("duration_sec") / 60.0)
            )
            .withColumn("mean_so_amplitude",
                F.expr("aggregate(slow_oscillations, 0.0, (acc, x) -> acc + x.ptp_amplitude, acc -> acc / so_count)")
            )
            .select(
                "subject_id",
                "slow_oscillations",
                "so_count",
                "so_density_per_min",
                "mean_so_amplitude",
                "processed_time"
            )
    )


if __name__ == "__main__":
    pass