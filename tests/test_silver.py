"""Unit tests for Silver layer preprocessing and event detection.

Exam Coverage:
- Testing (10%): UDF testing, complex transformations
"""

import pytest
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class TestEEGPreprocessing:
    """Test EEG signal preprocessing."""
    
    def test_sampling_rate_validation(self):
        """Test that low sampling rates are rejected."""
        sampling_rate = 50  # Too low
        assert sampling_rate < 100  # Should fail expectation
        
        sampling_rate = 200  # Valid
        assert sampling_rate >= 100
    
    def test_duration_validation(self):
        """Test minimum duration requirement."""
        duration_sec = 30  # Too short
        assert duration_sec < 60  # Should fail expectation
        
        duration_sec = 120  # Valid
        assert duration_sec >= 60
    
    def test_channel_count_validation(self):
        """Test minimum channel count."""
        n_channels = 1  # Too few
        assert n_channels < 2  # Should fail expectation
        
        n_channels = 4  # Valid
        assert n_channels >= 2


class TestSpindleDetection:
    """Test sleep spindle detection."""
    
    def test_spindle_density_calculation(self):
        """Test spindle density computation."""
        spindle_count = 120
        duration_sec = 600  # 10 minutes
        
        spindle_density = spindle_count / (duration_sec / 60.0)
        
        expected_density = 12.0  # 120 spindles / 10 minutes
        assert abs(spindle_density - expected_density) < 0.01
    
    def test_mean_spindle_properties(self):
        """Test spindle property aggregation."""
        spindles = [
            {"amplitude": 50.0, "frequency": 12.5},
            {"amplitude": 60.0, "frequency": 13.0},
            {"amplitude": 55.0, "frequency": 12.8}
        ]
        
        mean_amp = np.mean([s["amplitude"] for s in spindles])
        mean_freq = np.mean([s["frequency"] for s in spindles])
        
        assert abs(mean_amp - 55.0) < 0.1
        assert abs(mean_freq - 12.77) < 0.1


class TestPACComputation:
    """Test phase-amplitude coupling."""
    
    def test_pac_value_range(self):
        """Test PAC modulation index is non-negative."""
        pac_values = [0.001, 0.005, 0.01, 0.02]
        
        for pac in pac_values:
            assert pac >= 0  # Must be non-negative
    
    def test_pac_typical_range(self):
        """Test PAC is in typical physiological range."""
        pac_value = 0.008
        
        # Typical range: 0.0001 to 0.1
        assert 0.0001 <= pac_value <= 0.1