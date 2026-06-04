"""Silver Layer: Compute phase-amplitude coupling (PAC) between SOs and spindles.

Phase-amplitude coupling quantifies the synchronization between the phase
of slow oscillations (0.5-1 Hz) and the amplitude of sleep spindles (11-16 Hz).
Strong PAC is predictive of memory consolidation success.

References:
- Tort et al. (2010). Measuring phase-amplitude coupling between neuronal
  oscillations of different frequencies. J Neurophysiol, 104(2), 1195-1210.
  DOI: 10.1152/jn.00106.2010
- Ngo et al. (2020). Sleep spindles mediate hippocampal-neocortical coupling.
  eLife, 9, e57011. DOI: 10.7554/eLife.57011

Exam Coverage:
- Data Processing (30%): Complex numerical UDFs with scipy
- Data Modeling (20%): Aggregation and feature engineering
- Monitoring (10%): Value range validation
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def compute_pac_modulation_index(eeg_data: list, sfreq: float) -> float:
    """Compute PAC modulation index using Tort et al. (2010) method.
    
    Args:
        eeg_data: 2D array of EEG data (channels × samples)
        sfreq: Sampling frequency in Hz
    
    Returns:
        Modulation index (MI): KL divergence from uniform distribution.
        Higher MI indicates stronger phase-amplitude coupling.
        Typical range: 0.0001 to 0.01 (higher = stronger coupling)
    
    Method:
    1. Extract SO phase (0.5-1 Hz) via Hilbert transform
    2. Extract spindle amplitude (11-16 Hz) via Hilbert transform
    3. Bin SO phase into 18 bins (-π to π)
    4. Compute mean spindle amplitude per phase bin
    5. Normalize to probability distribution
    6. Compute KL divergence from uniform distribution
    """
    from scipy.signal import hilbert, butter, filtfilt
    import numpy as np
    
    try:
        # Use first channel
        data = np.array(eeg_data[0])
        
        # Bandpass filter helper
        def bandpass(signal, lowcut, highcut, fs, order=4):
            nyq = 0.5 * fs
            low = lowcut / nyq
            high = highcut / nyq
            b, a = butter(order, [low, high], btype='band')
            return filtfilt(b, a, signal)
        
        # Extract SO phase (0.5-1 Hz)
        so_filtered = bandpass(data, 0.5, 1.0, sfreq)
        so_phase = np.angle(hilbert(so_filtered))
        
        # Extract spindle amplitude (11-16 Hz)
        spindle_filtered = bandpass(data, 11, 16, sfreq)
        spindle_amp = np.abs(hilbert(spindle_filtered))
        
        # Bin SO phase into 18 bins (following Tort et al. 2010)
        n_bins = 18
        phase_bins = np.linspace(-np.pi, np.pi, n_bins + 1)
        
        # Compute mean spindle amplitude per phase bin
        amp_per_bin = []
        for i in range(n_bins):
            mask = (so_phase >= phase_bins[i]) & (so_phase < phase_bins[i + 1])
            if np.sum(mask) > 0:
                amp_per_bin.append(np.mean(spindle_amp[mask]))
            else:
                amp_per_bin.append(0.0)
        
        # Normalize to probability distribution
        p = np.array(amp_per_bin)
        p = p / (np.sum(p) + 1e-10)  # Add epsilon to avoid division by zero
        
        # Uniform distribution
        uniform = np.ones(n_bins) / n_bins
        
        # Compute modulation index (KL divergence)
        # MI = Σ p(j) * log(p(j) / uniform(j))
        mi = np.sum(p * np.log((p + 1e-10) / uniform))
        
        return float(mi)
    
    except Exception as e:
        return 0.0


# Register UDF
compute_pac_udf = F.udf(compute_pac_modulation_index, returnType=DoubleType())


@dlt.table(
    name="silver_pac",
    comment="""Phase-amplitude coupling (PAC) between slow oscillations and spindles.
    Quantifies SO-spindle synchronization, a validated memory consolidation biomarker.""",
    table_properties={"quality": "silver"},
    partition_cols=["subject_id"]
)
@dlt.expect_or_drop("valid_pac", "pac_modulation_index >= 0")
@dlt.expect("typical_pac_range", "pac_modulation_index BETWEEN 0.0001 AND 0.1", on_violation="warn")
def silver_pac():
    """Compute phase-amplitude coupling for each subject.
    
    Returns:
        DataFrame with PAC modulation index per subject.
    
    Data Quality:
    - valid_pac: MI must be non-negative (drop if violated)
    - typical_pac_range: MI typically 0.0001-0.1 (warn if outside)
    """
    return (
        dlt.read("silver_eeg_preprocessed")
            .withColumn("pac_modulation_index", compute_pac_udf(
                F.col("data"),
                F.col("sampling_rate")
            ))
            .select(
                "subject_id",
                "pac_modulation_index",
                F.current_timestamp().alias("computed_time")
            )
    )


if __name__ == "__main__":
    pass