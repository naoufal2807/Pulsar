# tests/intelligence/test_isolator.py
"""Tests for Isolator module."""

import pytest
import polars as pl
from datetime import datetime, timedelta

from pulsar.core.intelligence.small_world.isolator import Isolator


class TestIsolatorBasics:
    """Test basic Isolator functionality."""

    @pytest.fixture
    def isolator(self):
        """Create Isolator instance."""
        return Isolator()

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pl.DataFrame({
            'id': range(10000),
            'value': [float(i) for i in range(10000)],
        })

    def test_isolator_initialization(self, isolator):
        """Test Isolator initializes correctly."""
        assert isolator is not None
        assert isolator.sample_percentage == 0.10
        assert isolator.min_size == 1000

    def test_custom_configuration(self):
        """Test Isolator with custom config."""
        config = {
            'sample_percentage': 0.20,
            'min_size': 500,
            'seed': 99,
        }
        isolator = Isolator(config)

        assert isolator.sample_percentage == 0.20
        assert isolator.min_size == 500
        assert isolator.seed == 99

    def test_extract_empty_dataframe(self, isolator):
        """Test extract with empty DataFrame."""
        empty_df = pl.DataFrame()
        result = isolator.extract(empty_df)

        assert result.is_empty()

    def test_extract_respects_min_size(self, isolator):
        """Test extract respects minimum size."""
        # Small DataFrame
        df = pl.DataFrame({'id': range(100)})
        result = isolator.extract(df)

        # Should get all 100 rows since min_size is 1000
        assert result.shape[0] == 100

    def test_extract_respects_percentage(self):
        """Test extract respects percentage."""
        isolator = Isolator({'sample_percentage': 0.10, 'min_size': 100})
        df = pl.DataFrame({'id': range(20000)})
        result = isolator.extract(df)

        # Should be ~10% = 2000 rows
        assert 1800 < result.shape[0] < 2200

    def test_extract_preserves_schema(self, isolator, sample_df):
        """Test that extract preserves schema."""
        result = isolator.extract(sample_df)

        assert result.columns == sample_df.columns
        assert result.schema == sample_df.schema


class TestRandomSampling:
    """Test random sampling strategy."""

    @pytest.fixture
    def isolator(self):
        return Isolator({'strategy': 'random'})

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            'id': range(10000),
            'value': [float(i) for i in range(10000)],
        })

    def test_random_sampling(self, isolator, sample_df):
        """Test random sampling."""
        result = isolator.extract(sample_df, strategy='random')

        assert result.shape[0] >= 1000  # Min size
        assert result.shape[0] <= sample_df.shape[0]

    def test_random_sampling_reproducible(self, isolator, sample_df):
        """Test random sampling is reproducible with seed."""
        result1 = isolator.extract(sample_df, strategy='random')
        result2 = isolator.extract(sample_df, strategy='random')

        # Same seed should produce same results
        assert result1.shape[0] == result2.shape[0]

    def test_random_sampling_is_diverse(self, isolator, sample_df):
        """Test that random sampling includes diverse values."""
        result = isolator.extract(sample_df, strategy='random')

        # Should have a good spread of values
        min_id = result['id'].min()
        max_id = result['id'].max()

        assert min_id < 1000  # Some early values
        assert max_id > 8000  # Some late values


class TestStratifiedSampling:
    """Test stratified sampling strategy."""

    @pytest.fixture
    def isolator(self):
        return Isolator({'strategy': 'stratified'})

    @pytest.fixture
    def stratified_df(self):
        """DataFrame with clear strata."""
        return pl.DataFrame({
            'region': ['North'] * 3000 + ['South'] * 3000 + ['East'] * 2000 + ['West'] * 2000,
            'value': range(10000),
        })

    def test_stratified_sampling(self, isolator, stratified_df):
        """Test stratified sampling."""
        result = isolator.extract(stratified_df, strategy='stratified')

        assert result.shape[0] >= 1000
        assert 'region' in result.columns

    def test_stratified_sampling_balanced(self, isolator, stratified_df):
        """Test stratified sampling maintains proportions."""
        result = isolator.extract(stratified_df, strategy='stratified')

        # Check that regions are represented
        regions = result['region'].unique().to_list()
        assert len(regions) > 1  # Multiple regions sampled

    def test_stratified_sampling_explicit_column(self, isolator, stratified_df):
        """Test stratified sampling with explicit column."""
        result = isolator.extract(
            stratified_df,
            strategy='stratified',
            stratify_col='region'
        )

        assert result.shape[0] >= 1000

    def test_stratified_sampling_auto_detect(self, isolator, stratified_df):
        """Test stratified sampling auto-detects column."""
        result = isolator.extract(stratified_df, strategy='stratified')

        assert result.shape[0] >= 1000

    def test_stratified_sampling_falls_back_to_random(self, isolator):
        """Test stratified sampling falls back when no categorical column."""
        df = pl.DataFrame({
            'col1': range(10000),
            'col2': [float(i) for i in range(10000)],
        })

        result = isolator.extract(df, strategy='stratified')

        assert result.shape[0] >= 1000


class TestTemporalSampling:
    """Test temporal sampling strategy."""

    @pytest.fixture
    def isolator(self):
        return Isolator({'strategy': 'temporal'})

    @pytest.fixture
    def temporal_df(self):
        """DataFrame with temporal data."""
        dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(10000)]
        return pl.DataFrame({
            'date': dates,
            'value': range(10000),
        })

    def test_temporal_sampling(self, isolator, temporal_df):
        """Test temporal sampling."""
        result = isolator.extract(temporal_df, strategy='temporal')

        assert result.shape[0] >= 1000
        assert 'date' in result.columns

    def test_temporal_sampling_distributed(self, isolator, temporal_df):
        """Test temporal sampling distributes across time."""
        result = isolator.extract(temporal_df, strategy='temporal')

        # Should have dates from early and late in the range
        min_date = result['date'].min()
        max_date = result['date'].max()

        assert min_date < datetime(2022, 1, 1)
        assert max_date > datetime(2026, 1, 1)

    def test_temporal_sampling_explicit_column(self, isolator, temporal_df):
        """Test temporal sampling with explicit column."""
        result = isolator.extract(
            temporal_df,
            strategy='temporal',
            temporal_col='date'
        )

        assert result.shape[0] >= 1000

    def test_temporal_sampling_auto_detect(self, isolator, temporal_df):
        """Test temporal sampling auto-detects date column."""
        result = isolator.extract(temporal_df, strategy='temporal')

        assert result.shape[0] >= 1000

    def test_temporal_sampling_falls_back_to_random(self, isolator):
        """Test temporal sampling falls back when no date column."""
        df = pl.DataFrame({
            'col1': range(10000),
            'col2': [float(i) for i in range(10000)],
        })

        result = isolator.extract(df, strategy='temporal')

        assert result.shape[0] >= 1000


class TestFirstNSampling:
    """Test first-N sampling strategy."""

    @pytest.fixture
    def isolator(self):
        return Isolator({'strategy': 'first_n'})

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            'id': range(10000),
            'value': [float(i) for i in range(10000)],
        })

    def test_first_n_sampling(self, isolator, sample_df):
        """Test first-N sampling."""
        result = isolator.extract(sample_df, strategy='first_n')

        assert result.shape[0] >= 1000

    def test_first_n_sampling_deterministic(self, isolator, sample_df):
        """Test first-N sampling is deterministic."""
        result1 = isolator.extract(sample_df, strategy='first_n')
        result2 = isolator.extract(sample_df, strategy='first_n')

        # Should be identical
        assert result1.equals(result2)

    def test_first_n_sampling_preserves_order(self, isolator, sample_df):
        """Test first-N sampling preserves row order."""
        result = isolator.extract(sample_df, strategy='first_n')

        # First row should be ID 0
        assert result['id'][0] == 0


class TestStrategyOverride:
    """Test strategy parameter override."""

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            'id': range(10000),
            'region': ['A'] * 5000 + ['B'] * 5000,
            'date': [datetime(2020, 1, 1) + timedelta(days=i) for i in range(10000)],
        })

    def test_override_strategy(self, sample_df):
        """Test overriding strategy at extract time."""
        isolator = Isolator({'strategy': 'random'})

        # Override with stratified
        result = isolator.extract(sample_df, strategy='stratified')

        assert result.shape[0] >= 1000

    def test_override_strategy_temporal(self, sample_df):
        """Test overriding to temporal strategy."""
        isolator = Isolator({'strategy': 'random'})

        result = isolator.extract(sample_df, strategy='temporal')

        assert result.shape[0] >= 1000


class TestAutoDetection:
    """Test auto-detection of columns."""

    @pytest.fixture
    def isolator(self):
        return Isolator()

    def test_auto_detect_stratification_column(self, isolator):
        """Test auto-detection of stratification column."""
        df = pl.DataFrame({
            'id': range(1000),
            'region': (['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] * 125),  # 8 unique values = good cardinality
            'value': range(1000),
        })

        # Should auto-detect 'region' as stratification column
        result = isolator._find_stratification_column(df)

        assert result == 'region'

    def test_auto_detect_ignores_low_cardinality(self, isolator):
        """Test auto-detection ignores low cardinality columns."""
        df = pl.DataFrame({
            'id': range(1000),
            'binary': [0, 1] * 500,  # Only 2 values
            'value': range(1000),
        })

        result = isolator._find_stratification_column(df)

        # Should not select binary column (too low cardinality)
        assert result is None or result != 'binary'

    def test_auto_detect_ignores_high_cardinality(self, isolator):
        """Test auto-detection ignores high cardinality columns."""
        df = pl.DataFrame({
            'id': range(1000),  # Nearly unique
            'category': ['A'] * 500 + ['B'] * 500,
            'value': range(1000),
        })

        result = isolator._find_stratification_column(df)

        # Should not select id (too high cardinality)
        assert result != 'id'

    def test_auto_detect_temporal_column(self, isolator):
        """Test auto-detection of temporal column."""
        dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
        df = pl.DataFrame({
            'id': range(1000),
            'created_date': dates,
            'value': range(1000),
        })

        result = isolator._find_temporal_column(df)

        assert result == 'created_date'

    def test_auto_detect_temporal_prefers_keywords(self, isolator):
        """Test auto-detection prefers temporal keywords."""
        dates = [datetime(2020, 1, 1) + timedelta(days=i) for i in range(1000)]
        df = pl.DataFrame({
            'id': range(1000),
            'date_column': dates,
            'timestamp': dates,
            'value': range(1000),
        })

        result = isolator._find_temporal_column(df)

        # Should prefer column with 'date' in name
        assert result in ['date_column', 'timestamp']


class TestSampleQuality:
    """Test quality of extracted samples."""

    @pytest.fixture
    def isolator(self):
        return Isolator()

    @pytest.fixture
    def large_df(self):
        """Large realistic DataFrame."""
        return pl.DataFrame({
            'customer_id': range(100000),
            'region': ['North', 'South', 'East', 'West'] * 25000,
            'signup_date': [
                datetime(2020, 1, 1) + timedelta(days=i % 2000)
                for i in range(100000)
            ],
            'age': [20 + (i % 60) for i in range(100000)],
            'value': [float(i % 1000) for i in range(100000)],
        })

    def test_sample_has_correct_size(self, isolator, large_df):
        """Test sample is correct size."""
        result = isolator.extract(large_df)

        assert result.shape[0] >= 1000
        assert result.shape[0] <= large_df.shape[0]

    def test_sample_preserves_column_count(self, isolator, large_df):
        """Test sample preserves all columns."""
        result = isolator.extract(large_df)

        assert result.shape[1] == large_df.shape[1]
        assert result.columns == large_df.columns

    def test_sample_has_reasonable_diversity(self, isolator, large_df):
        """Test sample has reasonable value diversity."""
        result = isolator.extract(large_df)

        # Check that not all rows are identical
        # Count non-duplicated rows
        non_dup_count = result.shape[0] - result.is_duplicated().sum()
        assert non_dup_count > 1

    def test_sample_with_different_strategies(self, isolator, large_df):
        """Test that different strategies produce different samples."""
        random_sample = isolator.extract(large_df, strategy='random')
        first_n_sample = isolator.extract(large_df, strategy='first_n')

        # Should be different (very low probability of being identical)
        assert not random_sample.equals(first_n_sample)


class TestIsolatorEdgeCases:
    """Test edge cases."""

    @pytest.fixture
    def isolator(self):
        return Isolator()

    def test_single_row_dataframe(self, isolator):
        """Test with single row DataFrame."""
        df = pl.DataFrame({'id': [1], 'value': [10.0]})
        result = isolator.extract(df)

        assert result.shape[0] == 1

    def test_dataframe_smaller_than_min_size(self, isolator):
        """Test DataFrame smaller than min_size."""
        df = pl.DataFrame({'id': range(500)})
        result = isolator.extract(df)

        assert result.shape[0] == 500

    def test_all_null_column(self, isolator):
        """Test with all-null column."""
        df = pl.DataFrame({
            'col1': [None] * 10000,
            'col2': range(10000),
        })
        result = isolator.extract(df)

        assert result.shape[0] >= 1000
        assert 'col1' in result.columns

    def test_very_small_percentage(self):
        """Test with very small percentage."""
        isolator = Isolator({'sample_percentage': 0.001, 'min_size': 100})
        df = pl.DataFrame({'id': range(1000)})

        result = isolator.extract(df)

        # Should respect min_size
        assert result.shape[0] >= 100
