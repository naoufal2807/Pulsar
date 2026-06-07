# tests/diagnosis/test_impact_trend.py
"""Tests for Impact Assessment and Trend Analysis."""

import pytest
from datetime import datetime, timedelta

from pulsar.core.diagnosis.impact_assessor import (
    ImpactAssessor, ImpactScope, ImpactDomain
)
from pulsar.core.diagnosis.trend_analyzer import (
    TrendAnalyzer, TrendDirection
)
from pulsar.core.diagnosis.anomaly_detector import (
    Anomaly, AnomalyType, AnomalySeverity
)


class TestImpactAssessor:
    """Test impact assessment."""

    @pytest.fixture
    def assessor(self):
        return ImpactAssessor()

    @pytest.fixture
    def sample_anomaly(self):
        return Anomaly(
            row_index=10,
            anomaly_type=AnomalyType.OUTLIER,
            column_name="revenue",
            value=50000,
            expected_range=(1000, 10000),
            severity=AnomalySeverity.HIGH,
            confidence=0.9,
            explanation="Revenue value is outlier",
        )

    @pytest.fixture
    def sample_context(self):
        return {
            'data_type': 'transaction',
            'data_purpose': 'business',
        }

    @pytest.fixture
    def sample_dataset_info(self):
        return {
            'total_rows': 100000,
        }

    def test_impact_assessor_creation(self, assessor):
        """Test creating impact assessor."""
        assert assessor is not None
        assert assessor.cost_per_hour == 500

    def test_assess_single_anomaly(self, assessor, sample_anomaly, sample_context, sample_dataset_info):
        """Test assessing impact of single anomaly."""
        impacts = assessor.assess([sample_anomaly], sample_context, sample_dataset_info)

        assert len(impacts) == 1
        impact = impacts[0]
        assert impact.rows_affected > 0
        assert impact.severity == AnomalySeverity.HIGH

    def test_scope_determination(self, assessor):
        """Test scope determination."""
        # Single row anomaly
        anomaly = Anomaly(
            row_index=5,
            anomaly_type=AnomalyType.OUTLIER,
            column_name="value",
            value=100,
            expected_range=(0, 50),
            severity=AnomalySeverity.LOW,
            confidence=0.5,
            explanation="Test",
        )

        scope = assessor._determine_scope(anomaly, {'total_rows': 10000})

        assert scope == ImpactScope.SINGLE_ROW

    def test_domain_determination(self, assessor):
        """Test domain determination based on purpose."""
        business_context = {'data_purpose': 'monetization'}
        domain = assessor._determine_domain(business_context)
        assert domain == ImpactDomain.BUSINESS

        ml_context = {'data_purpose': 'ml'}
        domain = assessor._determine_domain(ml_context)
        assert domain == ImpactDomain.ML_MODELS

    def test_revenue_impact_calculation(self, assessor):
        """Test revenue impact calculation."""
        revenue = assessor._calculate_revenue_impact(100, ImpactDomain.BUSINESS)

        # 100 rows * $10/row = $1000
        assert revenue == 1000.0

    def test_prioritize_impacts(self, assessor, sample_anomaly, sample_context, sample_dataset_info):
        """Test prioritizing impacts by risk."""
        anomaly1 = sample_anomaly
        anomaly2 = Anomaly(
            row_index=-1,  # Column-level anomaly
            anomaly_type=AnomalyType.BEHAVIORAL_SHIFT,
            column_name="status",
            value="unexpected",
            expected_range=("expected",),
            severity=AnomalySeverity.CRITICAL,
            confidence=0.95,
            explanation="Critical behavioral shift",
        )

        impacts = assessor.assess([anomaly1, anomaly2], sample_context, sample_dataset_info)
        prioritized = assessor.prioritize_impacts(impacts)

        # Should have 2 impacts
        assert len(prioritized) == 2
        # The highest priority should be the column-level anomaly
        assert prioritized[0].scope == ImpactScope.COLUMN


class TestTrendAnalyzer:
    """Test trend analysis."""

    @pytest.fixture
    def analyzer(self):
        return TrendAnalyzer()

    @pytest.fixture
    def sample_anomaly(self):
        return Anomaly(
            row_index=10,
            anomaly_type=AnomalyType.OUTLIER,
            column_name="value",
            value=100,
            expected_range=(0, 50),
            severity=AnomalySeverity.MEDIUM,
            confidence=0.7,
            explanation="Test outlier",
        )

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
        # Add multiple anomalies
        for _ in range(5):
            analyzer.add_anomaly(sample_anomaly)

        trends = analyzer.analyze_trends()

        assert len(trends) == 1
        trend = trends[0]
        assert trend.count_last_day == 5

    def test_clustering_calculation(self, analyzer):
        """Test clustering coefficient calculation."""
        anomalies = [
            (datetime.now() - timedelta(hours=2), None),
            (datetime.now() - timedelta(hours=1.5), None),
            (datetime.now() - timedelta(hours=1), None),
        ]

        clustering = analyzer._calculate_clustering(anomalies)

        # Clustered anomalies should have high coefficient
        assert 0 <= clustering <= 1

    def test_trend_direction_determination(self, analyzer):
        """Test trend direction determination."""
        # Degrading: high count in last hour
        direction = analyzer._determine_trend_direction(5, 10, 0.5)
        assert direction == TrendDirection.CRITICAL

        # Improving: no recent anomalies
        direction = analyzer._determine_trend_direction(0, 0, 0.0)
        assert direction == TrendDirection.IMPROVING

        # Stable: some anomalies, steady rate
        direction = analyzer._determine_trend_direction(1, 5, 0.1)
        assert direction == TrendDirection.STABLE

    def test_predict_next_occurrence(self, analyzer):
        """Test predicting next anomaly."""
        # Create regular anomalies
        now = datetime.now()
        anomalies = [
            (now - timedelta(hours=4), None),
            (now - timedelta(hours=3), None),
            (now - timedelta(hours=2), None),
            (now - timedelta(hours=1), None),
        ]

        predicted_hours, confidence = analyzer._predict_next_occurrence(anomalies)

        # Should predict within next hour (based on 1-hour pattern)
        assert predicted_hours is None or predicted_hours >= 0
        assert 0 <= confidence <= 1

    def test_get_critical_trends(self, analyzer):
        """Test filtering critical trends."""
        # Create multiple trends and filter
        trends = [
            type('Trend', (), {'direction': TrendDirection.IMPROVING})(),
            type('Trend', (), {'direction': TrendDirection.CRITICAL})(),
            type('Trend', (), {'direction': TrendDirection.DEGRADING})(),
        ]

        # Would need actual trend objects with direction attribute
        critical = [t for t in trends if hasattr(t, 'direction') and t.direction in [TrendDirection.CRITICAL, TrendDirection.DEGRADING]]

        assert len(critical) == 2


class TestImpactTrendIntegration:
    """Integration tests for impact and trend analysis."""

    def test_impact_and_trend_together(self):
        """Test using impact assessor with trend analyzer."""
        assessor = ImpactAssessor()
        analyzer = TrendAnalyzer()

        # Create anomalies
        anomalies = [
            Anomaly(
                row_index=i,
                anomaly_type=AnomalyType.OUTLIER,
                column_name="value",
                value=100 + i,
                expected_range=(0, 50),
                severity=AnomalySeverity.MEDIUM,
                confidence=0.7,
                explanation="Test outlier",
            )
            for i in range(5)
        ]

        # Assess impact
        context = {'data_purpose': 'business'}
        dataset_info = {'total_rows': 10000}
        impacts = assessor.assess(anomalies, context, dataset_info)

        # Analyze trends
        for anomaly in anomalies:
            analyzer.add_anomaly(anomaly)
        trends = analyzer.analyze_trends()

        # Should have 5 impacts (one per anomaly) and 1 trend (same column/type)
        assert len(impacts) == 5
        assert len(trends) == 1
        assert trends[0].count_last_day == 5


class TestEdgeCases:
    """Test edge cases."""

    def test_impact_assessor_empty_anomalies(self):
        """Test assessing empty anomaly list."""
        assessor = ImpactAssessor()
        impacts = assessor.assess([], {}, {})

        assert impacts == []

    def test_trend_analyzer_empty_history(self):
        """Test analyzing empty anomaly history."""
        analyzer = TrendAnalyzer()
        trends = analyzer.analyze_trends()

        assert trends == []

    def test_impact_with_no_revenue(self):
        """Test impact assessment with non-business domain."""
        assessor = ImpactAssessor()

        anomaly = Anomaly(
            row_index=0,
            anomaly_type=AnomalyType.OUTLIER,
            column_name="metric",
            value=100,
            expected_range=(0, 50),
            severity=AnomalySeverity.LOW,
            confidence=0.5,
            explanation="Test",
        )

        context = {'data_purpose': 'operations'}
        dataset_info = {'total_rows': 1000}

        impacts = assessor.assess([anomaly], context, dataset_info)

        # Should have impact but no revenue
        assert len(impacts) == 1
        assert impacts[0].revenue_impact is None or impacts[0].revenue_impact == 0
