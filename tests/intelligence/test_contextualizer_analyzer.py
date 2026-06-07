# tests/intelligence/test_contextualizer_analyzer.py
"""Tests for Contextualizer and Analyzer modules."""

import pytest
import polars as pl

from pulsar.core.intelligence.small_world.contextualizer import (
    Contextualizer, DataType, DataPurpose, QualityTier
)
from pulsar.core.intelligence.small_world.analyzer import (
    Analyzer, Understanding
)


class TestContextualizerBasics:
    """Test basic Contextualizer functionality."""

    @pytest.fixture
    def contextualizer(self):
        return Contextualizer()

    @pytest.fixture
    def customer_patterns(self):
        """Patterns typical of customer data."""
        return {
            'formats': {
                'email': {
                    'email': {
                        'match_ratio': 0.95,
                        'examples': ['user@example.com'],
                    }
                }
            },
            'ranges': {
                'age': {'type': 'numeric', 'min': 18, 'max': 100},
                'phone': {'type': 'categorical', 'cardinality_ratio': 0.98},
                'region': {'type': 'categorical', 'cardinality_ratio': 0.1, 'top_values': [('US', 100), ('EU', 50)]},
            },
            'distributions': {
                'age': {'skewness': 0.5, 'kurtosis': -0.2},
            },
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {
                    'duplicate_ratio': 0.01,
                    'null_ratio_by_column': {},
                },
            },
        }

    def test_contextualizer_initialization(self, contextualizer):
        """Test Contextualizer initializes correctly."""
        assert contextualizer is not None

    def test_contextualize_basic(self, contextualizer, customer_patterns):
        """Test basic contextualization."""
        context = contextualizer.contextualize(customer_patterns, coverage_ratio=0.95)

        assert 'data_type' in context
        assert 'data_purpose' in context
        assert 'quality_tier' in context
        assert 'validation_rules' in context
        assert 'quality_requirements' in context
        assert 'characteristics' in context

    def test_infer_customer_data_type(self, contextualizer, customer_patterns):
        """Test inferring customer data type."""
        data_type = contextualizer.infer_data_type(customer_patterns)

        assert data_type == DataType.CUSTOMER

    def test_infer_transaction_data_type(self, contextualizer):
        """Test inferring transaction data type."""
        patterns = {
            'ranges': {
                'amount': {'type': 'numeric'},
                'total': {'type': 'numeric'},
                'price': {'type': 'numeric'},
            },
            'formats': {},
            'distributions': {},
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {},
            },
        }

        data_type = contextualizer.infer_data_type(patterns)

        assert data_type == DataType.TRANSACTION

    def test_infer_data_purpose(self, contextualizer):
        """Test data purpose inference."""
        patterns = {
            'ranges': {
                'revenue': {'type': 'numeric'},
                'price': {'type': 'numeric'},
            },
            'formats': {},
            'distributions': {},
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {},
            },
        }

        purpose = contextualizer.infer_data_purpose(patterns)

        assert purpose == DataPurpose.MONETIZATION

    def test_infer_quality_tier_based_on_coverage(self, contextualizer, customer_patterns):
        """Test quality tier inference based on coverage."""
        tier_real_time = contextualizer.infer_quality_tier(customer_patterns, 0.9999)
        tier_critical = contextualizer.infer_quality_tier(customer_patterns, 0.991)
        tier_high = contextualizer.infer_quality_tier(customer_patterns, 0.96)
        tier_medium = contextualizer.infer_quality_tier(customer_patterns, 0.93)

        assert tier_real_time == QualityTier.REAL_TIME
        assert tier_critical == QualityTier.CRITICAL
        assert tier_high == QualityTier.HIGH
        assert tier_medium == QualityTier.MEDIUM

    def test_build_validation_rules(self, contextualizer, customer_patterns):
        """Test building validation rules."""
        rules = contextualizer.build_validation_rules(customer_patterns)

        assert 'format_rules' in rules
        assert 'range_rules' in rules
        assert 'relationship_rules' in rules
        assert 'cardinality_rules' in rules

    def test_extract_quality_requirements(self, contextualizer, customer_patterns):
        """Test extracting quality requirements."""
        requirements = contextualizer.extract_quality_requirements(customer_patterns, 0.95)

        assert 'minimum_coverage' in requirements
        assert 'maximum_null_ratio' in requirements
        assert 'duplicate_tolerance' in requirements

    def test_extract_characteristics(self, contextualizer):
        """Test characteristic extraction."""
        patterns = {
            'ranges': {
                'metric1': {'type': 'numeric'},
                'metric2': {'type': 'numeric'},
                'metric3': {'type': 'numeric'},
                'metric4': {'type': 'numeric'},
                'metric5': {'type': 'numeric'},
                'metric6': {'type': 'numeric'},
            },
            'patterns': {
                'temporal_patterns': {'date': {}},
                'categorical_patterns': {},
                'data_quality_indicators': {},
            },
        }

        characteristics = contextualizer._extract_characteristics(patterns.get('patterns', {}))

        # Should detect multiple characteristics
        assert isinstance(characteristics, dict)


class TestAnalyzerBasics:
    """Test basic Analyzer functionality."""

    @pytest.fixture
    def analyzer(self):
        return Analyzer()

    @pytest.fixture
    def high_quality_patterns(self):
        """High quality patterns."""
        return {
            'formats': {
                'email': {'email': {'match_ratio': 0.99}},
            },
            'ranges': {
                'age': {'type': 'numeric', 'min': 18, 'max': 100},
            },
            'distributions': {
                'age': {'skewness': 0.1, 'kurtosis': -0.2, 'outliers_iqr': {'outlier_count': 2, 'outlier_percentage': 0.2}},
            },
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {
                    'duplicate_ratio': 0.001,
                    'null_ratio_by_column': {},
                },
            },
            'relationships': {},
        }

    @pytest.fixture
    def high_quality_context(self):
        """High quality context."""
        return {
            'data_type': DataType.CUSTOMER,
            'data_purpose': DataPurpose.ACQUISITION,
            'quality_tier': QualityTier.HIGH,
            'characteristics': {'has_temporal_dimension': False, 'is_high_volume': False},
        }

    @pytest.fixture
    def good_expansion(self):
        """Good expansion results."""
        return {
            'summary': {
                'total_rows': 1000,
                'valid_rows': 980,
                'invalid_rows': 20,
                'coverage_ratio': 0.98,
            },
            'metrics': [],
        }

    def test_analyzer_initialization(self, analyzer):
        """Test Analyzer initializes correctly."""
        assert analyzer is not None

    def test_analyze_generates_understanding(self, analyzer, high_quality_patterns, high_quality_context, good_expansion):
        """Test analyze generates Understanding object."""
        understanding = analyzer.analyze(high_quality_patterns, high_quality_context, good_expansion)

        assert isinstance(understanding, Understanding)
        assert understanding.summary is not None
        assert understanding.data_type is not None
        assert understanding.quality_score >= 0
        assert understanding.quality_score <= 100

    def test_quality_score_calculation(self, analyzer, high_quality_patterns, good_expansion):
        """Test quality score is calculated correctly."""
        score = analyzer.calculate_quality_score(high_quality_patterns, good_expansion)

        assert isinstance(score, float)
        assert 0 <= score <= 100
        # Good quality should score high
        assert score > 70

    def test_score_to_grade_conversion(self, analyzer):
        """Test score to grade conversion."""
        assert analyzer._score_to_grade(95) == "A"
        assert analyzer._score_to_grade(85) == "B"
        assert analyzer._score_to_grade(75) == "C"
        assert analyzer._score_to_grade(65) == "D"
        assert analyzer._score_to_grade(50) == "F"

    def test_identify_strengths(self, analyzer, high_quality_patterns, good_expansion):
        """Test identifying strengths."""
        strengths = analyzer._identify_strengths(high_quality_patterns, good_expansion)

        assert isinstance(strengths, list)
        assert len(strengths) > 0
        # Good data should have strengths
        assert any('coverage' in s or 'format' in s or 'completeness' in s for s in strengths)

    def test_identify_weaknesses_in_bad_data(self, analyzer):
        """Test identifying weaknesses in poor quality data."""
        bad_patterns = {
            'formats': {},
            'ranges': {},
            'distributions': {
                'age': {
                    'outliers_iqr': {'outlier_count': 100, 'outlier_percentage': 10},
                },
            },
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {
                    'duplicate_ratio': 0.2,
                    'null_ratio_by_column': {'col1': 0.5},
                },
            },
        }

        bad_expansion = {
            'summary': {
                'total_rows': 1000,
                'valid_rows': 500,
                'invalid_rows': 500,
                'coverage_ratio': 0.5,
            },
        }

        weaknesses = analyzer._identify_weaknesses(bad_patterns, bad_expansion)

        assert len(weaknesses) > 0
        assert any('low coverage' in w or 'high' in w for w in weaknesses)

    def test_generate_recommendations(self, analyzer):
        """Test generating recommendations."""
        bad_patterns = {
            'formats': {},
            'ranges': {},
            'distributions': {},
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {
                    'duplicate_ratio': 0.1,
                    'null_ratio_by_column': {'email': 0.2},
                },
            },
        }

        bad_expansion = {
            'summary': {
                'invalid_rows': 100,
            },
        }

        weaknesses = ['low coverage', 'high duplicates']

        recommendations = analyzer._generate_recommendations(bad_patterns, bad_expansion, weaknesses)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0

    def test_generate_summary_statement(self, analyzer):
        """Test summary statement generation."""
        summary = analyzer._generate_summary('customer', 'acquisition', 'A', 0.98)

        assert 'customer' in summary.lower()
        assert 'acquisition' in summary.lower()
        assert 'A' in summary
        assert '98' in summary

    def test_compare_understandings(self, analyzer, high_quality_patterns, high_quality_context, good_expansion):
        """Test comparing two understandings."""
        baseline = analyzer.analyze(high_quality_patterns, high_quality_context, good_expansion)

        # Create slightly degraded version
        degraded_expansion = {
            'summary': {
                'total_rows': 1000,
                'valid_rows': 950,
                'invalid_rows': 50,
                'coverage_ratio': 0.95,
            },
            'metrics': [],
        }

        current = analyzer.analyze(high_quality_patterns, high_quality_context, degraded_expansion)

        comparison = analyzer.compare(baseline, current)

        assert 'status' in comparison
        assert 'delta_quality' in comparison
        assert 'delta_coverage' in comparison


class TestEndToEndAnalysis:
    """Integration tests for complete analysis."""

    @pytest.fixture
    def analyzer(self):
        return Analyzer()

    @pytest.fixture
    def contextualizer(self):
        return Contextualizer()

    def test_complete_analysis_pipeline(self, analyzer, contextualizer):
        """Test complete analysis from patterns to understanding."""
        # Create realistic patterns
        patterns = {
            'formats': {
                'email': {'email': {'match_ratio': 0.98}},
                'phone': {'phone_us': {'match_ratio': 0.95}},
            },
            'ranges': {
                'customer_id': {'type': 'numeric', 'min': 1, 'max': 10000},
                'age': {'type': 'numeric', 'min': 18, 'max': 95},
                'lifetime_value': {'type': 'numeric', 'min': 0, 'max': 50000},
                'region': {'type': 'categorical', 'cardinality_ratio': 0.05, 'top_values': [('US', 500), ('EU', 300)]},
            },
            'distributions': {
                'age': {'skewness': 0.3, 'kurtosis': 0.1, 'outliers_iqr': {'outlier_count': 5, 'outlier_percentage': 0.5}},
                'lifetime_value': {'skewness': 2.1, 'kurtosis': 5.0, 'outliers_iqr': {'outlier_count': 50, 'outlier_percentage': 5.0}},
            },
            'relationships': {
                'age': [('lifetime_value', 0.65)],
            },
            'patterns': {
                'temporal_patterns': {'signup_date': {}},
                'categorical_patterns': {'region': {'is_dominated': False}},
                'data_quality_indicators': {
                    'duplicate_ratio': 0.02,
                    'null_ratio_by_column': {'phone': 0.01},
                },
            },
        }

        expansion_result = {
            'summary': {
                'total_rows': 10000,
                'valid_rows': 9800,
                'invalid_rows': 200,
                'coverage_ratio': 0.98,
            },
            'metrics': [],
        }

        # Step 1: Contextualize
        context = contextualizer.contextualize(patterns, expansion_result['summary']['coverage_ratio'])

        # Step 2: Analyze
        understanding = analyzer.analyze(patterns, context, expansion_result)

        # Verify complete understanding
        assert understanding.summary is not None
        assert understanding.quality_score > 80
        assert understanding.quality_grade in ['A', 'B', 'C', 'D', 'F']
        assert len(understanding.strengths) > 0
        assert len(understanding.recommendations) >= 0


class TestAnalyzerEdgeCases:
    """Test edge cases."""

    @pytest.fixture
    def analyzer(self):
        return Analyzer()

    @pytest.fixture
    def contextualizer(self):
        return Contextualizer()

    def test_analyze_empty_patterns(self, analyzer, contextualizer):
        """Test analysis with empty patterns."""
        patterns = {
            'formats': {},
            'ranges': {},
            'distributions': {},
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {},
            },
            'relationships': {},
        }

        context = contextualizer.contextualize(patterns, 0.9)

        expansion = {
            'summary': {
                'coverage_ratio': 0.9,
            },
        }

        understanding = analyzer.analyze(patterns, context, expansion)

        assert understanding is not None
        assert 0 <= understanding.quality_score <= 100

    def test_quality_score_with_all_nulls(self, analyzer):
        """Test quality score when all columns have nulls."""
        patterns = {
            'formats': {},
            'ranges': {},
            'distributions': {},
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {
                    'null_ratio_by_column': {
                        'col1': 1.0,
                        'col2': 1.0,
                        'col3': 1.0,
                    },
                    'duplicate_ratio': 0,
                },
            },
            'relationships': {},
        }

        expansion = {
            'summary': {
                'coverage_ratio': 0,
            },
        }

        score = analyzer.calculate_quality_score(patterns, expansion)

        # Should be very low
        assert score < 50

    def test_compare_identical_understandings(self, analyzer, contextualizer):
        """Test comparing identical understandings."""
        patterns = {
            'formats': {},
            'ranges': {},
            'distributions': {},
            'patterns': {
                'temporal_patterns': {},
                'categorical_patterns': {},
                'data_quality_indicators': {},
            },
            'relationships': {},
        }

        context = contextualizer.contextualize(patterns, 0.9)
        expansion = {'summary': {'coverage_ratio': 0.9}, 'metrics': []}

        understanding = analyzer.analyze(patterns, context, expansion)

        comparison = analyzer.compare(understanding, understanding)

        assert comparison['delta_quality'] == 0.0
        assert comparison['status'] == 'stable'
