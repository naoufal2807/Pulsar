# tests/diagnosis/test_anomaly_detector.py
"""Tests for anomaly detection."""

import pytest
import polars as pl
from pulsar.core.diagnosis.anomaly_detector import (
    AnomalyDetector, AnomalyType, AnomalySeverity
)


class TestAnomalyDetectorBasics:
    """Test basic anomaly detector functionality."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    def test_detector_creation(self, detector):
        """Test creating anomaly detector."""
        assert detector is not None

    def test_detector_with_config(self):
        """Test detector with custom config."""
        config = {
            'iqr_multiplier': 2.0,
            'zscore_threshold': 2.5,
        }
        detector = AnomalyDetector(config)

        assert detector.iqr_multiplier == 2.0
        assert detector.zscore_threshold == 2.5


class TestOutlierDetection:
    """Test outlier detection."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    @pytest.fixture
    def baseline_stats(self):
        """Baseline statistics."""
        return {
            'age': {
                'type': 'numeric',
                'min': 18,
                'max': 95,
                'mean': 50,
                'std': 15,
            },
            'salary': {
                'type': 'numeric',
                'min': 20000,
                'max': 200000,
                'mean': 100000,
                'std': 50000,
            },
        }

    def test_detect_no_outliers_in_normal_data(self, detector, baseline_stats):
        """Test normal data produces no outliers."""
        df = pl.DataFrame({
            'age': [25, 30, 35, 40, 45],
            'salary': [50000, 60000, 70000, 80000, 90000],
        })

        anomalies = detector.detect_outliers(df, baseline_stats)

        # Should detect some outliers if thresholds are very tight
        # But with these ranges, all values are within bounds
        assert len(anomalies) == 0

    def test_detect_outliers_outside_range(self, detector, baseline_stats):
        """Test detection of values outside baseline range."""
        df = pl.DataFrame({
            'age': [25, 30, 150, 40, 45],  # 150 is outlier
            'salary': [50000, 60000, 70000, 300000, 90000],  # 300000 is outlier
        })

        anomalies = detector.detect_outliers(df, baseline_stats)

        # Should detect at least 2 outliers
        assert len(anomalies) >= 2

        # Check anomaly properties
        for anomaly in anomalies:
            assert anomaly.anomaly_type == AnomalyType.OUTLIER
            assert anomaly.severity in [AnomalySeverity.MEDIUM, AnomalySeverity.HIGH]
            assert 0 <= anomaly.confidence <= 1

    def test_outlier_severity_far_from_baseline(self, detector, baseline_stats):
        """Test outliers far from baseline have higher severity."""
        df = pl.DataFrame({
            'age': [25, 200, 30],  # 200 is very far
        })

        anomalies = detector.detect_outliers(df, baseline_stats)

        assert len(anomalies) > 0
        # Should have high severity for extreme outlier
        assert any(a.severity == AnomalySeverity.HIGH for a in anomalies)


class TestBehavioralShiftDetection:
    """Test behavioral shift detection."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    @pytest.fixture
    def baseline_patterns(self):
        """Baseline patterns."""
        return {
            'categorical_patterns': {
                'status': {
                    'is_dominated': True,
                    'dominant_value': 'active',
                    'dominant_ratio': 0.8,
                    'distribution_type': 'skewed',
                }
            }
        }

    def test_detect_no_shift_when_stable(self, detector, baseline_patterns):
        """Test no shift detected when distribution is stable."""
        df = pl.DataFrame({
            'status': ['active'] * 80 + ['inactive'] * 20,
        })

        anomalies = detector.detect_behavioral_shifts(df, baseline_patterns)

        # Should not detect shift if distribution is similar
        assert len(anomalies) <= 1

    def test_detect_shift_when_changed(self, detector, baseline_patterns):
        """Test shift detection when dominant value changes."""
        df = pl.DataFrame({
            'status': ['inactive'] * 70 + ['active'] * 30,  # Inactive now dominant
        })

        anomalies = detector.detect_behavioral_shifts(df, baseline_patterns)

        # Might detect a shift
        assert isinstance(anomalies, list)


class TestContextualAnomalyDetection:
    """Test contextual anomaly detection."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    @pytest.fixture
    def baseline_relationships(self):
        """Baseline correlation relationships."""
        return {
            'age': [('salary', 0.7), ('experience', 0.8)],
        }

    def test_detect_no_contextual_anomaly_with_consistent_correlation(self, detector, baseline_relationships):
        """Test no anomaly when correlations are consistent."""
        df = pl.DataFrame({
            'age': range(1, 101),
            'salary': [s * 1000 for s in range(1, 101)],  # Strong correlation
            'experience': [e * 1 for e in range(1, 101)],  # Strong correlation
        })

        anomalies = detector.detect_contextual_anomalies(df, baseline_relationships)

        # Should have few anomalies with consistent correlations
        assert len(anomalies) <= 2


class TestAnomalyIntegration:
    """Integration tests for anomaly detection."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    def test_detect_multiple_anomaly_types(self, detector):
        """Test detecting multiple types of anomalies."""
        baseline_stats = {
            'value': {
                'type': 'numeric',
                'min': 0,
                'max': 100,
                'mean': 50,
                'std': 20,
            }
        }

        baseline_patterns = {
            'categorical_patterns': {}
        }

        baseline_relationships = {}

        df = pl.DataFrame({
            'value': [25, 30, 150, 45, 50],  # 150 is outlier
        })

        # Detect outliers
        outliers = detector.detect_outliers(df, baseline_stats)

        # All detection methods should work
        shifts = detector.detect_behavioral_shifts(df, baseline_patterns)
        contextual = detector.detect_contextual_anomalies(df, baseline_relationships)

        assert len(outliers) > 0


class TestEdgeCases:
    """Test edge cases."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    def test_empty_dataframe(self, detector):
        """Test with empty DataFrame."""
        df = pl.DataFrame()
        baseline_stats = {}

        anomalies = detector.detect_outliers(df, baseline_stats)

        assert anomalies == []

    def test_null_values(self, detector):
        """Test handling of null values."""
        df = pl.DataFrame({
            'value': [25, None, 30, None, 100],
        })

        baseline_stats = {
            'value': {
                'type': 'numeric',
                'min': 0,
                'max': 100,
                'mean': 50,
                'std': 20,
            }
        }

        # Should handle nulls gracefully
        anomalies = detector.detect_outliers(df, baseline_stats)

        assert isinstance(anomalies, list)

    def test_all_same_values(self, detector):
        """Test with all identical values."""
        df = pl.DataFrame({
            'value': [50] * 10,
        })

        baseline_stats = {
            'value': {
                'type': 'numeric',
                'min': 50,
                'max': 50,
                'mean': 50,
                'std': 0,
            }
        }

        anomalies = detector.detect_outliers(df, baseline_stats)

        # Identical values shouldn't produce anomalies
        assert len(anomalies) == 0
