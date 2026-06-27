# tests/diagnosis/test_impact_trend.py
"""Tests for Impact Assessment and Trend Analysis."""

import pytest
from datetime import datetime, timedelta

from pulsar.core.diagnosis.impact_assessor import ImpactAssessor
from pulsar.core.diagnosis.trend_analyzer import TrendAnalyzer, TrendDirection
from pulsar.core.diagnosis.models import Anomaly, AnomalyType, AnomalySeverity


def make_anomaly(column_name="revenue", anomaly_type=AnomalyType.OUTLIER,
                 severity=3, rows_affected=1):
    return Anomaly(
        anomaly_type=anomaly_type,
        column_name=column_name,
        value=50000.0,
        severity=severity,
        confidence=0.9,
        description=f"Test anomaly in {column_name}",
        rows_affected=rows_affected,
    )


class TestImpactAssessor:
    """Test impact assessment."""

    @pytest.fixture
    def assessor(self):
        return ImpactAssessor()

    @pytest.fixture
    def sample_anomaly(self):
        return make_anomaly()

    def test_impact_assessor_creation(self, assessor):
        """Test creating impact assessor."""
        assert assessor is not None
        assert assessor.cost_per_hour == 500

    def test_assess_single_anomaly(self, assessor, sample_anomaly):
        """Test assessing impact of single anomaly."""
        impacts = assessor.assess([sample_anomaly], df_shape=(100, 5))

        assert len(impacts) == 1
        impact = impacts[0]
        assert impact.affected_rows > 0
        assert impact.impact_score >= 0

    def test_assess_empty_anomalies(self, assessor):
        """Test assessing empty anomaly list."""
        impacts = assessor.assess([], df_shape=(1000, 10))
        assert impacts == []

    def test_assess_returns_impact_per_anomaly(self, assessor):
        """Test that assess returns one Impact per anomaly."""
        anomalies = [make_anomaly("col1"), make_anomaly("col2")]
        impacts = assessor.assess(anomalies, df_shape=(1000, 5))
        assert len(impacts) == 2

    def test_impact_type_data_quality_for_outlier(self, assessor):
        """Test outliers map to data_quality impact type."""
        anomaly = make_anomaly(anomaly_type=AnomalyType.OUTLIER)
        impacts = assessor.assess([anomaly], df_shape=(100, 3))

        assert impacts[0].impact_type == "data_quality"

    def test_impact_type_business_for_behavioral_shift(self, assessor):
        """Test behavioral shifts map to business impact type."""
        anomaly = make_anomaly(anomaly_type=AnomalyType.BEHAVIORAL_SHIFT)
        impacts = assessor.assess([anomaly], df_shape=(100, 3))

        assert impacts[0].impact_type == "business"

    def test_higher_severity_raises_risk_score(self, assessor):
        """Test that higher severity anomalies have higher impact scores."""
        low = make_anomaly(severity=int(AnomalySeverity.LOW))
        high = make_anomaly(severity=int(AnomalySeverity.HIGH))

        impacts_low = assessor.assess([low], df_shape=(1000, 5))
        impacts_high = assessor.assess([high], df_shape=(1000, 5))

        assert impacts_high[0].impact_score > impacts_low[0].impact_score

    def test_recovery_effort_within_bounds(self, assessor, sample_anomaly):
        """Test recovery effort is within 0-10 bounds."""
        impacts = assessor.assess([sample_anomaly], df_shape=(100, 5))
        assert 0 <= impacts[0].recovery_effort <= 10


class TestTrendAnalyzer:
    """Test trend analysis."""

    @pytest.fixture
    def analyzer(self):
        return TrendAnalyzer()

    @pytest.fixture
    def sample_anomaly(self):
        return make_anomaly(column_name="value", severity=int(AnomalySeverity.MEDIUM))

    def test_trend_analyzer_creation(self, analyzer):
        """Test creating trend analyzer."""
        assert analyzer is not None
        assert analyzer.anomaly_history == []

    def test_add_anomaly(self, analyzer, sample_anomaly):
        """Test adding anomaly to history."""
        analyzer.add_anomaly(sample_anomaly)
        assert len(analyzer.anomaly_history) == 1

    def test_analyze_single_anomaly(self, analyzer, sample_anomaly):
        """Test analyzing trend with single anomaly."""
        analyzer.add_anomaly(sample_anomaly)
        trends = analyzer.analyze_trends()

        assert len(trends) == 1
        trend = trends[0]
        assert trend.column_name == "value"
        assert trend.count_last_day == 1

    def test_analyze_multiple_anomalies(self, analyzer, sample_anomaly):
        """Test analyzing trend with multiple anomalies."""
        for _ in range(5):
            analyzer.add_anomaly(sample_anomaly)

        trends = analyzer.analyze_trends()

        assert len(trends) == 1
        trend = trends[0]
        assert trend.count_last_day == 5

    def test_analyze_empty_history(self, analyzer):
        """Test analyzing empty anomaly history."""
        trends = analyzer.analyze_trends()
        assert trends == []

    def test_clustering_calculation(self, analyzer):
        """Test clustering coefficient calculation."""
        anomalies = [
            (datetime.now() - timedelta(hours=2), None),
            (datetime.now() - timedelta(hours=1, minutes=30), None),
            (datetime.now() - timedelta(hours=1), None),
        ]

        clustering = analyzer._calculate_clustering(anomalies)
        assert 0 <= clustering <= 1

    def test_trend_direction_critical(self, analyzer):
        """Test CRITICAL direction when many anomalies in last hour."""
        direction = analyzer._determine_trend_direction(5, 10, 0.5)
        assert direction == TrendDirection.CRITICAL

    def test_trend_direction_improving(self, analyzer):
        """Test IMPROVING direction when no anomalies in last day."""
        direction = analyzer._determine_trend_direction(0, 0, 0.0)
        assert direction == TrendDirection.IMPROVING

    def test_trend_direction_stable(self, analyzer):
        """Test STABLE direction for moderate, steady rate."""
        direction = analyzer._determine_trend_direction(1, 5, 0.1)
        assert direction == TrendDirection.STABLE

    def test_predict_next_occurrence(self, analyzer):
        """Test predicting next anomaly."""
        now = datetime.now()
        anomalies = [
            (now - timedelta(hours=4), None),
            (now - timedelta(hours=3), None),
            (now - timedelta(hours=2), None),
            (now - timedelta(hours=1), None),
        ]

        predicted_hours, confidence = analyzer._predict_next_occurrence(anomalies)

        assert predicted_hours is None or predicted_hours >= 0
        assert 0 <= confidence <= 1

    def test_predict_single_anomaly_returns_none(self, analyzer):
        """Test that single anomaly gives no prediction."""
        anomalies = [(datetime.now(), None)]
        predicted_hours, confidence = analyzer._predict_next_occurrence(anomalies)
        assert predicted_hours is None
        assert confidence == 0.0


class TestImpactTrendIntegration:
    """Integration tests for impact and trend analysis."""

    def test_impact_and_trend_together(self):
        """Test using impact assessor with trend analyzer."""
        assessor = ImpactAssessor()
        analyzer = TrendAnalyzer()

        anomalies = [make_anomaly("value", severity=int(AnomalySeverity.MEDIUM))
                     for _ in range(5)]

        impacts = assessor.assess(anomalies, df_shape=(10000, 5))

        for anomaly in anomalies:
            analyzer.add_anomaly(anomaly)
        trends = analyzer.analyze_trends()

        assert len(impacts) == 5
        assert len(trends) == 1
        assert trends[0].count_last_day == 5
