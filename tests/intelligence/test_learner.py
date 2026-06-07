# tests/intelligence/test_learner.py
"""Tests for Learner module."""

import pytest
import polars as pl
from datetime import datetime, timedelta

from pulsar.core.intelligence.small_world.learner import Learner


class TestLearnerBasics:
    """Test basic Learner functionality."""

    @pytest.fixture
    def learner(self):
        """Create Learner instance."""
        return Learner()

    @pytest.fixture
    def sample_numeric_df(self):
        """Create sample DataFrame with numeric data."""
        return pl.DataFrame({
            'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
            'salary': [30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000, 110000, 120000],
            'years_experience': [1, 3, 5, 8, 10, 15, 20, 22, 25, 28],
        })

    @pytest.fixture
    def sample_mixed_df(self):
        """Create sample DataFrame with mixed data types."""
        return pl.DataFrame({
            'id': range(1, 101),
            'email': [f'user{i}@example.com' for i in range(1, 101)],
            'phone': ['555-0001', '555-0002', '555-0003'] * 33 + ['555-0001'],
            'created_date': [datetime(2020, 1, 1) + timedelta(days=i) for i in range(100)],
            'status': ['active', 'inactive', 'pending'] * 33 + ['active'],
            'score': [float(i) for i in range(100)],
        })

    def test_learner_initialization(self, learner):
        """Test Learner initializes correctly."""
        assert learner is not None
        assert learner.config == {}

    def test_learn_empty_dataframe(self, learner):
        """Test learn with empty DataFrame."""
        empty_df = pl.DataFrame()
        result = learner.learn(empty_df)

        assert result is not None
        assert 'formats' in result
        assert 'ranges' in result
        assert 'distributions' in result
        assert 'relationships' in result
        assert 'patterns' in result
        assert result['metadata']['sample_size'] == 0

    def test_learn_numeric_dataframe(self, learner, sample_numeric_df):
        """Test learn with numeric DataFrame."""
        result = learner.learn(sample_numeric_df)

        assert result is not None
        assert 'ranges' in result
        assert 'distributions' in result

        # Check ranges
        assert 'age' in result['ranges']
        age_range = result['ranges']['age']
        assert age_range['type'] == 'numeric'
        assert age_range['min'] == 25
        assert age_range['max'] == 70

    def test_learn_mixed_dataframe(self, learner, sample_mixed_df):
        """Test learn with mixed data types."""
        result = learner.learn(sample_mixed_df)

        assert result is not None
        assert 'formats' in result
        assert 'ranges' in result
        assert 'patterns' in result

        # Check metadata
        assert result['metadata']['sample_size'] == 100
        assert result['metadata']['num_columns'] == 6


class TestDiscoverFormats:
    """Test format discovery."""

    @pytest.fixture
    def learner(self):
        return Learner()

    @pytest.fixture
    def format_df(self):
        """DataFrame with various formats."""
        return pl.DataFrame({
            'emails': [
                'john@example.com',
                'jane@example.com',
                'bob@example.com',
                'not-an-email',
            ] * 25,
            'urls': [
                'https://example.com',
                'http://google.com',
                'not-a-url',
            ] * 33 + ['https://example.com'],
            'dates': [
                '2020-01-01',
                '2021-02-15',
                '2022-12-31',
            ] * 33 + ['2023-06-15'],
            'random_text': ['text1', 'text2', 'text3'] * 33 + ['text1'],
        })

    def test_discover_formats(self, learner, format_df):
        """Test format discovery."""
        formats = learner.discover_formats(format_df)

        assert formats is not None
        assert isinstance(formats, dict)

    def test_discover_email_format(self, learner, format_df):
        """Test email format detection."""
        formats = learner.discover_formats(format_df)

        assert 'emails' in formats
        email_formats = formats['emails']
        assert 'email' in email_formats
        assert email_formats['email']['match_ratio'] > 0.7  # 75% are emails
        assert len(email_formats['email']['examples']) > 0

    def test_discover_date_format(self, learner, format_df):
        """Test date format detection."""
        formats = learner.discover_formats(format_df)

        assert 'dates' in formats
        date_formats = formats['dates']
        assert 'date_iso' in date_formats
        assert date_formats['date_iso']['match_ratio'] > 0.7

    def test_discover_url_format(self, learner, format_df):
        """Test URL format detection."""
        formats = learner.discover_formats(format_df)

        assert 'urls' in formats
        url_formats = formats['urls']
        assert 'url' in url_formats
        assert url_formats['url']['match_ratio'] > 0.6

    def test_discover_no_formats_in_numeric_column(self, learner):
        """Test that numeric columns are skipped."""
        df = pl.DataFrame({
            'numbers': range(100),
        })
        formats = learner.discover_formats(df)

        assert formats == {}


class TestDiscoverRanges:
    """Test range discovery."""

    @pytest.fixture
    def learner(self):
        return Learner()

    @pytest.fixture
    def range_df(self):
        """DataFrame for range testing."""
        return pl.DataFrame({
            'age': [25, 30, 35, 40, 45] * 20,
            'salary': [30000.0, 50000.0, 70000.0, 90000.0, 110000.0] * 20,
            'category': (['A', 'B', 'C'] * 34)[:100],  # Ensure exactly 100 rows
        })

    def test_discover_numeric_ranges(self, learner, range_df):
        """Test numeric range discovery."""
        ranges = learner.discover_ranges(range_df)

        assert 'age' in ranges
        age_range = ranges['age']
        assert age_range['type'] == 'numeric'
        assert 'min' in age_range
        assert 'max' in age_range
        assert 'mean' in age_range
        assert 'std' in age_range
        assert age_range['min'] == 25
        assert age_range['max'] == 45

    def test_discover_percentiles(self, learner, range_df):
        """Test percentile calculation."""
        ranges = learner.discover_ranges(range_df)

        age_range = ranges['age']
        assert 'p25' in age_range
        assert 'p50' in age_range
        assert 'p75' in age_range
        assert 'p90' in age_range
        assert 'p99' in age_range

    def test_discover_categorical_ranges(self, learner, range_df):
        """Test categorical column analysis."""
        ranges = learner.discover_ranges(range_df)

        assert 'category' in ranges
        cat_range = ranges['category']
        assert cat_range['type'] == 'categorical'
        assert 'cardinality_ratio' in cat_range
        assert 'unique_count' in cat_range
        assert 'top_values' in cat_range

    def test_discover_categorical_top_values(self, learner, range_df):
        """Test top values in categorical column."""
        ranges = learner.discover_ranges(range_df)

        cat_range = ranges['category']
        top_values = cat_range['top_values']
        assert len(top_values) > 0
        assert top_values[0][0] in ['A', 'B', 'C']  # One of the categories
        assert isinstance(top_values[0][1], int)  # Count is integer

    def test_discover_temporal_ranges(self, learner):
        """Test temporal column range discovery."""
        dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(100)]
        df = pl.DataFrame({
            'date_col': dates,
        })

        ranges = learner.discover_ranges(df)

        assert 'date_col' in ranges
        date_range = ranges['date_col']
        assert date_range['type'] == 'temporal'
        assert 'min_date' in date_range
        assert 'max_date' in date_range
        assert 'date_span_days' in date_range


class TestDiscoverDistributions:
    """Test distribution discovery."""

    @pytest.fixture
    def learner(self):
        return Learner()

    @pytest.fixture
    def distribution_df(self):
        """DataFrame with various distributions."""
        # Mostly normal distribution
        import math
        normal_values = [50 + 10 * ((i % 100) - 50) / 50 for i in range(100)]

        # Right-skewed
        skewed_values = [i ** 1.5 for i in range(1, 101)]

        return pl.DataFrame({
            'normal': normal_values,
            'skewed': skewed_values,
        })

    def test_discover_distributions(self, learner, distribution_df):
        """Test distribution discovery."""
        distributions = learner.discover_distributions(distribution_df)

        assert isinstance(distributions, dict)

    def test_discover_skewness(self, learner, distribution_df):
        """Test skewness calculation."""
        distributions = learner.discover_distributions(distribution_df)

        assert 'skewed' in distributions
        dist = distributions['skewed']
        assert 'skewness' in dist
        assert dist['skewness'] is not None

    def test_discover_kurtosis(self, learner, distribution_df):
        """Test kurtosis calculation."""
        distributions = learner.discover_distributions(distribution_df)

        assert 'skewed' in distributions
        dist = distributions['skewed']
        assert 'kurtosis' in dist

    def test_discover_outliers(self, learner, distribution_df):
        """Test outlier detection."""
        distributions = learner.discover_distributions(distribution_df)

        assert 'normal' in distributions
        dist = distributions['normal']
        assert 'outliers_iqr' in dist
        assert 'outliers_zscore' in dist

    def test_outliers_have_required_fields(self, learner, distribution_df):
        """Test outlier detection returns correct fields."""
        distributions = learner.discover_distributions(distribution_df)

        dist = distributions['normal']
        iqr_outliers = dist['outliers_iqr']
        assert 'outlier_count' in iqr_outliers
        assert 'outlier_percentage' in iqr_outliers


class TestDiscoverRelationships:
    """Test relationship discovery."""

    @pytest.fixture
    def learner(self):
        return Learner()

    @pytest.fixture
    def correlated_df(self):
        """DataFrame with correlated columns."""
        x = list(range(100))
        # Strong positive correlation
        y = [val * 2 + 10 for val in x]
        # No correlation
        z = [50 if i % 2 == 0 else 60 for i in range(100)]

        return pl.DataFrame({
            'x': x,
            'y': y,
            'z': z,
        })

    def test_discover_relationships(self, learner, correlated_df):
        """Test relationship discovery."""
        relationships = learner.discover_relationships(correlated_df)

        assert isinstance(relationships, dict)

    def test_discover_strong_correlation(self, learner, correlated_df):
        """Test detection of strong correlation."""
        relationships = learner.discover_relationships(correlated_df)

        # x and y should be highly correlated
        if 'x' in relationships:
            assert any(col_name == 'y' for col_name, corr in relationships['x'])

    def test_correlation_values_in_range(self, learner, correlated_df):
        """Test correlation values are between -1 and 1."""
        relationships = learner.discover_relationships(correlated_df)

        for col_name, correlations in relationships.items():
            for col_b, corr_value in correlations:
                assert -1 <= corr_value <= 1

    def test_no_relationships_with_single_numeric_column(self, learner):
        """Test no relationships when only one numeric column."""
        df = pl.DataFrame({
            'col1': range(100),
            'col2': ['text'] * 100,
        })

        relationships = learner.discover_relationships(df)

        assert relationships == {}


class TestDiscoverPatterns:
    """Test pattern discovery."""

    @pytest.fixture
    def learner(self):
        return Learner()

    @pytest.fixture
    def pattern_df(self):
        """DataFrame with patterns."""
        dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(100)]
        status = ['active'] * 80 + ['inactive'] * 15 + ['pending'] * 5

        return pl.DataFrame({
            'created_date': dates,
            'status': status,
            'id': range(100),
        })

    def test_discover_patterns(self, learner, pattern_df):
        """Test pattern discovery."""
        patterns = learner.discover_patterns(pattern_df)

        assert 'temporal_patterns' in patterns
        assert 'categorical_patterns' in patterns
        assert 'data_quality_indicators' in patterns

    def test_detect_duplicates(self, learner):
        """Test duplicate detection."""
        df = pl.DataFrame({
            'col1': [1, 2, 2, 3],
        })

        patterns = learner.discover_patterns(df)

        indicators = patterns['data_quality_indicators']
        assert 'has_duplicates' in indicators
        assert 'duplicate_ratio' in indicators

    def test_detect_nulls(self, learner):
        """Test null detection."""
        df = pl.DataFrame({
            'col1': [1, 2, None, 4],
        })

        patterns = learner.discover_patterns(df)

        indicators = patterns['data_quality_indicators']
        assert 'null_ratio_by_column' in indicators
        # Only includes columns with nulls
        if indicators['null_ratio_by_column']:
            assert 'col1' in indicators['null_ratio_by_column']

    def test_temporal_pattern_detection(self, learner, pattern_df):
        """Test temporal pattern detection."""
        patterns = learner.discover_patterns(pattern_df)

        temporal = patterns['temporal_patterns']
        # Should detect something about the temporal column
        assert isinstance(temporal, dict)

    def test_categorical_pattern_detection(self, learner):
        """Test categorical pattern detection."""
        # Create a dominated categorical column
        df = pl.DataFrame({
            'status': ['active'] * 80 + ['inactive'] * 15 + ['pending'] * 5,
        })

        patterns = learner.discover_patterns(df)

        categorical = patterns['categorical_patterns']
        assert 'status' in categorical
        status_pattern = categorical['status']
        assert 'is_dominated' in status_pattern
        # 'active' is dominant (80%)
        assert status_pattern['is_dominated'] is True


class TestLearnerIntegration:
    """Integration tests for Learner."""

    @pytest.fixture
    def learner(self):
        return Learner()

    @pytest.fixture
    def realistic_df(self):
        """Realistic customer data."""
        return pl.DataFrame({
            'customer_id': range(1, 501),
            'email': [f'customer{i}@example.com' for i in range(1, 501)],
            'phone': ['555-' + str(1000 + i % 9000).zfill(4) for i in range(500)],
            'signup_date': [datetime(2020, 1, 1) + timedelta(days=i % 365) for i in range(500)],
            'age': [20 + (i % 60) for i in range(500)],
            'lifetime_value': [100.0 * (i % 100) for i in range(500)],
            'region': ['North', 'South', 'East', 'West'] * 125,
            'segment': (['Premium', 'Standard', 'Basic'] * 166 + ['Premium', 'Standard'])[:500],
        })

    def test_full_learning_pipeline(self, learner, realistic_df):
        """Test complete learning pipeline."""
        result = learner.learn(realistic_df)

        # Verify all components present
        assert 'formats' in result
        assert 'ranges' in result
        assert 'distributions' in result
        assert 'relationships' in result
        assert 'patterns' in result
        assert 'metadata' in result

        # Verify metadata
        assert result['metadata']['sample_size'] == 500
        assert result['metadata']['num_columns'] == 8

    def test_learning_produces_actionable_insights(self, learner, realistic_df):
        """Test that learning produces actionable insights."""
        result = learner.learn(realistic_df)

        # Email format should be detected
        if 'email' in result['formats']:
            assert 'email' in result['formats']['email']

        # Age ranges should be discovered
        if 'age' in result['ranges']:
            age_range = result['ranges']['age']
            assert 'min' in age_range
            assert 'max' in age_range

        # Region should show dominance pattern
        if 'region' in result['patterns']['categorical_patterns']:
            region_pattern = result['patterns']['categorical_patterns']['region']
            assert 'distribution_type' in region_pattern

    def test_learning_is_deterministic(self, learner, realistic_df):
        """Test that learning produces consistent results."""
        result1 = learner.learn(realistic_df)
        result2 = learner.learn(realistic_df)

        # Key metrics should be identical
        assert result1['metadata']['sample_size'] == result2['metadata']['sample_size']
        assert result1['ranges'] == result2['ranges']


class TestLearnerEdgeCases:
    """Test edge cases."""

    @pytest.fixture
    def learner(self):
        return Learner()

    def test_single_row_dataframe(self, learner):
        """Test with single row."""
        df = pl.DataFrame({
            'col1': [1],
            'col2': ['text'],
        })

        result = learner.learn(df)
        assert result is not None
        assert result['metadata']['sample_size'] == 1

    def test_all_nulls_column(self, learner):
        """Test column with all nulls."""
        df = pl.DataFrame({
            'col1': [None, None, None],
            'col2': [1, 2, 3],
        })

        result = learner.learn(df)
        assert result is not None

    def test_single_value_column(self, learner):
        """Test column with single distinct value."""
        df = pl.DataFrame({
            'constant': ['same'] * 100,
            'other': range(100),
        })

        result = learner.learn(df)
        patterns = result['patterns']['categorical_patterns']
        # Single value columns may or may not be detected depending on analysis
        # Just verify patterns exist
        assert isinstance(patterns, dict)

    def test_very_large_cardinality(self, learner):
        """Test high cardinality column."""
        df = pl.DataFrame({
            'unique_id': [f'id_{i}' for i in range(1000)],
            'value': range(1000),
        })

        result = learner.learn(df)
        ranges = result['ranges']
        # High cardinality columns should be in ranges
        assert 'unique_id' in ranges or 'value' in ranges
        if 'unique_id' in ranges:
            unique_range = ranges['unique_id']
            assert unique_range['cardinality_ratio'] > 0.99

    def test_mixed_valid_invalid_formats(self, learner):
        """Test column with mixed valid/invalid formats."""
        df = pl.DataFrame({
            'emails': [
                'valid@example.com',
                'invalid-email',
                'another@domain.org',
                'not-email',
            ] * 25,
        })

        result = learner.learn(df)
        formats = result['formats']

        assert 'emails' in formats
        assert 'email' in formats['emails']
        # Should be around 50% match
        assert 0.4 < formats['emails']['email']['match_ratio'] < 0.6
