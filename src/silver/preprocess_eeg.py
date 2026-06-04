"""Silver Layer: EEG signal preprocessing using MNE-Python.

This module preprocesses raw EDF files:
- Bandpass filtering (0.5-40 Hz)
- Notch filtering (50 Hz power line interference)
- Basic artifact rejection
- Channel standardization

References:
- Gramfort et al. (2013). MEG and EEG data analysis with MNE-Python.
  Front Neurosci, 7, 267. DOI: 10.3389/fnins.2013.00267
- Vallat & Walker (2021). YASA preprocessing pipeline.
  eLife, 10, e70092. DOI: 10.7554/eLife.70092

Exam Coverage:
- Data Processing (30%): Complex Spark UDFs, data transformations
- Data Modeling (20%): Nested struct types, array handling
- Monitoring (10%): Data quality expectations
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, StringType


def preprocess_eeg_signal(binary_content: bytes) -> dict:
    """Parse EDF file and apply preprocessing.
    
    Args:
        binary_content: Raw bytes from EDF file
    
    Returns:
        dict with keys:
        - channels: List of channel names
        - data: 2D array of preprocessed signals (channels × samples)
        - sampling_rate: Sampling frequency in Hz
        - duration_sec: Recording duration in seconds
        - n_channels: Number of EEG channels
        - n_samples: Number of time samples
    
    Preprocessing steps:
    1. Load EDF using MNE
    2. Bandpass filter 0.5-40 Hz (zero-phase FIR)
    3. Notch filter at 50 Hz (Europe) or 60 Hz (US)
    4. Reject flat channels (std < 0.1 µV)
    """
    import io
    import mne
    import numpy as np
    
    # Suppress MNE logging
    mne.set_log_level('ERROR')
    
    try:
        # Load EDF from binary content
        raw = mne.io.read_raw_edf(io.BytesIO(binary_content), preload=True, verbose=False)
        
        # Apply bandpass filter (0.5-40 Hz, zero-phase FIR)
        raw.filter(l_freq=0.5, h_freq=40, method='fir', phase='zero', verbose=False)
        
        # Apply notch filter (50 Hz for Europe)
        raw.notch_filter(freqs=50, method='fir', phase='zero', verbose=False)
        
        # Get channel names and data
        channels = raw.ch_names
        data = raw.get_data()  # Shape: (n_channels, n_samples)
        sfreq = raw.info['sfreq']
        
        # Basic artifact rejection: Remove flat channels
        channel_stds = np.std(data, axis=1)
        valid_channels = channel_stds > 0.1  # Threshold: 0.1 µV
        
        data = data[valid_channels]
        channels = [ch for i, ch in enumerate(channels) if valid_channels[i]]
        
        return {
            "channels": channels,
            "data": data.tolist(),  # Convert to list for PySpark
            "sampling_rate": float(sfreq),
            "duration_sec": float(data.shape[1] / sfreq),
            "n_channels": int(len(channels)),
            "n_samples": int(data.shape[1])
        }
    
    except Exception as e:
        # Return empty result on parsing error
        return {
            "channels": [],
            "data": [],
            "sampling_rate": 0.0,
            "duration_sec": 0.0,
            "n_channels": 0,
            "n_samples": 0,
            "error": str(e)
        }


# Define UDF return schema
preprocessed_schema = StructType([
    StructField("channels", ArrayType(StringType()), False),
    StructField("data", ArrayType(ArrayType(DoubleType())), False),
    StructField("sampling_rate", DoubleType(), False),
    StructField("duration_sec", DoubleType(), False),
    StructField("n_channels", DoubleType(), False),
    StructField("n_samples", DoubleType(), False)
])

# Register UDF
preprocess_eeg_udf = F.udf(preprocess_eeg_signal, returnType=preprocessed_schema)


@dlt.table(
    name="silver_eeg_preprocessed",
    comment="""Preprocessed EEG signals from PSG files.
    Bandpass filtered (0.5-40 Hz), notch filtered (50 Hz), artifact-rejected.""",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    },
    partition_cols=["subject_id"]
)
@dlt.expect_or_drop("valid_sampling_rate", "sampling_rate >= 100")
@dlt.expect_or_drop("sufficient_duration", "duration_sec >= 60")
@dlt.expect_or_drop("sufficient_channels", "n_channels >= 2")
@dlt.expect("standard_sampling_rate", "sampling_rate IN (100, 200, 250, 500)", on_violation="warn")
def silver_eeg_preprocessed():
    """Preprocess raw EEG signals.
    
    Returns:
        DataFrame with preprocessed EEG signals and metadata.
    
    Data Quality Expectations:
    - valid_sampling_rate: Must be >= 100 Hz (drop if violated)
    - sufficient_duration: Must be >= 60 seconds (drop if violated)
    - sufficient_channels: Must have >= 2 channels (drop if violated)
    - standard_sampling_rate: Should be 100/200/250/500 Hz (warn if violated)
    """
    return (
        dlt.read_stream("bronze_sleep_edf_raw")
            .filter(F.col("file_type") == "PSG")  # Only PSG files (not Hypnogram)
            # Apply preprocessing UDF
            .withColumn("preprocessed", preprocess_eeg_udf(F.col("content")))
            # Expand nested struct
            .select(
                "subject_id",
                "path",
                F.col("preprocessed.channels").alias("channels"),
                F.col("preprocessed.data").alias("data"),
                F.col("preprocessed.sampling_rate").alias("sampling_rate"),
                F.col("preprocessed.duration_sec").alias("duration_sec"),
                F.col("preprocessed.n_channels").alias("n_channels"),
                F.col("preprocessed.n_samples").alias("n_samples"),
                F.current_timestamp().alias("processed_time")
            )
            # Filter out failed preprocessing (sampling_rate = 0)
            .filter(F.col("sampling_rate") > 0)
    )


if __name__ == "__main__":
    # Module runs as part of DLT pipeline
    pass