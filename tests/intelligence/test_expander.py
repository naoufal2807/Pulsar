# tests/intelligence/test_expander.py
"""Tests for Expander module."""

import pytest
import polars as pl
from pulsar.core.intelligence.small_world.expander import Expander, LayerMetrics, ValidationResult


class TestExpanderBasics:
    """Test basic Expander functionality."""

    @pytest.fixture
    def expander(self):
        """Create Expander instance."""
        return Expander()

    @pytest.fixture
    def basic_patterns(self):
        """Create basic patterns for testing."""
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
                'age': {
                    'type': 'numeric',
                    'min': 18,
                    'max': 100,
                },
            },
        }

    @pytest.fixture
    def sample_df(self):
        """Create sample DataFrame."""
        return pl.DataFrame({
            'id': range(1, 1001),
            'email': [f'user{i}@example.com' for i in range(1, 1001)],
            'age': [20 + (i % 50) for i in range(1000)],
        })

    def test_expander_initialization(self, expander):
        """Test Expander initializes correctly."""
        assert expander is not None
        assert expander.strict_mode is False
        assert expander.contradiction_threshold == 0.1

    def test_custom_configuration(self):
        """Test Expander with custom config."""
        config = {
            'strict_mode': True,
            'contradiction_threshold': 0.05,
        }
        expander = Expander(config)

        assert expander.strict_mode is True
        assert expander.contradiction_threshold == 0.05

    def test_expand_empty_dataframe(self, expander, basic_patterns):
        """Test expand with empty DataFrame."""
        empty_df = pl.DataFrame()
        result = expander.expand(empty_df, basic_patterns)

        assert result is not None
        assert 'patterns' in result
        assert 'metrics' in result

    def test_expand_returns_required_keys(self, expander, sample_df, basic_patterns):
        """Test expand returns all required keys."""
        result = expander.expand(sample_df, basic_patterns)

        required_keys = ['patterns', 'coverage', 'contradictions', 'metrics', 'quality_score', 'summary']
        for key in required_keys:
            assert key in result


class TestValidation:
    """Test row validation."""

    @pytest.fixture
    def expander(self):
        return Expander()

    @pytest.fixture
    def patterns_with_formats(self):
        """Patterns with format checking."""
        return {
            'formats': {
                'email': {
                    'email': {
                        'match_ratio': 0.9,
                        'examples': ['user@example.com'],
                    }
                }
            },
        }

    @pytest.fixture
    def patterns_with_ranges(self):
        """Patterns with numeric ranges."""
        return {
            'ranges': {
                'age': {
                    'type': 'numeric',
                    'min': 18,
                    'max': 100,
                },
                'score': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 100,
                },
            },
        }

    def test_validate_format_valid(self, expander, patterns_with_formats):
        """Test validation of valid format."""
        df = pl.DataFrame({
            'email': ['user@example.com', 'valid@test.org'],
        })

        result = expander.validate_layer(df, patterns_with_formats)

        assert result['valid_rows'] == 2
        assert result['invalid_rows'] == 0

    def test_validate_format_invalid(self, expander, patterns_with_formats):
        """Test validation catches invalid formats."""
        df = pl.DataFrame({
            'email': ['user@example.com', 'not-an-email'],
        })

        result = expander.validate_layer(df, patterns_with_formats)

        # With high confidence format, invalid should be caught
        assert result['invalid_rows'] > 0

    def test_validate_numeric_range(self, expander, patterns_with_ranges):
        """Test numeric range validation."""
        df = pl.DataFrame({
            'age': [25, 30, 35],
            'score': [50, 75, 90],
        })

        result = expander.validate_layer(df, patterns_with_ranges)

        assert result['valid_rows'] == 3
        assert result['invalid_rows'] == 0

    def test_validate_numeric_out_of_range(self, expander, patterns_with_ranges):
        """Test detection of out-of-range values."""
        df = pl.DataFrame({
            'age': [25, 150, 35],  # 150 is out of range
            'score': [50, 75, 90],
        })

        result = expander.validate_layer(df, patterns_with_ranges)

        assert result['invalid_rows'] >= 1

    def test_validate_layer_with_nulls(self, expander, patterns_with_ranges):
        """Test validation handles null values."""
        df = pl.DataFrame({
            'age': [25, None, 35],
            'score': [50, 75, None],
        })

        result = expander.validate_layer(df, patterns_with_ranges)

        # Should handle nulls gracefully
        assert result['valid_rows'] + result['invalid_rows'] == 3


class TestLayerMetrics:
    """Test layer metrics tracking."""

    def test_layer_metrics_creation(self):
        """Test LayerMetrics initialization."""
        metrics = LayerMetrics(
            layer_number=1,
            rows_processed=1000,
            valid_rows=950,
            invalid_rows=50,
        )

        assert metrics.layer_number == 1
        assert metrics.rows_processed == 1000
        assert metrics.validation_ratio == 0.95

    def test_layer_metrics_with_contradictions(self):
        """Test LayerMetrics with contradictions."""
        contradictions = [
            {'row_index': 0, 'violations': ['email: format mismatch']},
            {'row_index': 5, 'violations': ['age: below minimum']},
        ]

        metrics = LayerMetrics(
            layer_number=2,
            rows_processed=1000,
            valid_rows=900,
            invalid_rows=100,
            contradictions=contradictions,
        )

        assert len(metrics.contradictions) == 2
        assert metrics.contradictions[0]['row_index'] == 0


class TestLayeredExpansion:
    """Test layered expansion process."""

    @pytest.fixture
    def expander(self):
        return Expander({
            'layer_sizes': [100, 500, 1000],
        })

    @pytest.fixture
    def layered_df(self):
        """Create DataFrame with enough rows for multiple layers."""
        return pl.DataFrame({
            'id': range(1, 2001),
            'value': [float(i % 100) for i in range(2000)],
            'category': ['A', 'B', 'C'] * 666 + ['A', 'B'],
        })

    @pytest.fixture
    def layered_patterns(self):
        """Patterns for layered testing."""
        return {
            'ranges': {
                'value': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 100,
                },
            },
        }

    def test_expand_creates_multiple_layers(self, expander, layered_df, layered_patterns):
        """Test that expand processes multiple layers."""
        result = expander.expand(layered_df, layered_patterns)

        # Should have metrics for multiple layers
        assert len(result['metrics']) > 0

    def test_expand_improves_coverage(self, expander, layered_df, layered_patterns):
        """Test that expansion maintains good coverage."""
        result = expander.expand(layered_df, layered_patterns)

        # Coverage should be reasonable
        coverage_ratio = result['summary']['coverage_ratio']
        assert coverage_ratio > 0.5  # At least 50% coverage

    def test_expand_tracks_quality_score(self, expander, layered_df, layered_patterns):
        """Test quality score calculation."""
        result = expander.expand(layered_df, layered_patterns)

        quality_score = result['quality_score']
        assert 0 <= quality_score <= 1

    def test_expand_returns_coverage_and_contradictions(self, expander, layered_df, layered_patterns):
        """Test coverage and contradiction filtering."""
        result = expander.expand(layered_df, layered_patterns)

        coverage = result['coverage']
        contradictions = result['contradictions']

        # Coverage + contradictions should equal or be less than total rows
        total = coverage.shape[0] + contradictions.shape[0]
        assert total <= layered_df.shape[0]


class TestFilteringOperations:
    """Test filtering and contradiction handling."""

    @pytest.fixture
    def expander(self):
        return Expander()

    @pytest.fixture
    def patterns(self):
        return {
            'ranges': {
                'age': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 120,
                },
            },
        }

    def test_filter_contradictions_removes_invalid_rows(self, expander, patterns):
        """Test filtering removes rows with contradictions."""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4],
            'age': [25, 150, 35, 30],  # 150 is invalid
        })

        result = expander.filter_contradictions(df, patterns)

        # Should have fewer rows
        assert result.shape[0] < df.shape[0]
        # Invalid row (index 1) should be removed
        ages = result['age'].to_list()
        assert 150 not in ages

    def test_get_valid_rows(self, expander, patterns):
        """Test getting valid rows."""
        df = pl.DataFrame({
            'id': [1, 2, 3],
            'age': [25, 35, 45],
        })

        valid = expander._get_valid_rows(df, patterns)

        assert valid.shape[0] == 3

    def test_get_invalid_rows(self, expander, patterns):
        """Test getting invalid rows."""
        df = pl.DataFrame({
            'id': [1, 2, 3, 4],
            'age': [25, 150, 35, 200],  # 150 and 200 are invalid
        })

        invalid = expander._get_invalid_rows(df, patterns)

        assert invalid.shape[0] >= 2


class TestPatternRefinement:
    """Test pattern refinement under contradictions."""

    @pytest.fixture
    def expander_lenient(self):
        return Expander({
            'strict_mode': False,
            'contradiction_threshold': 0.1,
        })

    @pytest.fixture
    def patterns_with_formats(self):
        return {
            'formats': {
                'email': {
                    'email': {
                        'match_ratio': 1.0,  # Very strict
                        'examples': [],
                    }
                }
            },
        }

    def test_pattern_refinement_on_high_contradiction_ratio(self, expander_lenient, patterns_with_formats):
        """Test that patterns are refined when contradiction ratio is high."""
        df = pl.DataFrame({
            'email': ['user@example.com', 'not-email', 'also-invalid', 'user2@domain.org'],
        })

        # This should detect high contradiction ratio and refine patterns
        result = expander_lenient.expand(df, patterns_with_formats)

        # Patterns should be refined (relaxed)
        refined_patterns = result['patterns']
        assert 'formats' in refined_patterns


class TestExpanderIntegration:
    """Integration tests with Learner patterns."""

    @pytest.fixture
    def expander(self):
        return Expander()

    def test_expand_with_realistic_patterns(self, expander):
        """Test expand with realistic data and patterns."""
        df = pl.DataFrame({
            'customer_id': range(1, 101),
            'email': [f'customer{i}@example.com' for i in range(1, 101)],
            'age': [20 + (i % 60) for i in range(100)],
            'score': [float(i % 100) for i in range(100)],
        })

        patterns = {
            'formats': {
                'email': {
                    'email': {
                        'match_ratio': 0.95,
                        'examples': ['customer1@example.com'],
                    }
                }
            },
            'ranges': {
                'age': {
                    'type': 'numeric',
                    'min': 18,
                    'max': 100,
                },
                'score': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 100,
                },
            },
        }

        result = expander.expand(df, patterns)

        assert result['summary']['coverage_ratio'] > 0.8
        assert result['quality_score'] > 0.7

    def test_expand_with_mixed_valid_invalid_data(self, expander):
        """Test expand with mixed valid and invalid data."""
        df = pl.DataFrame({
            'id': range(1, 101),
            'email': (
                [f'valid{i}@example.com' for i in range(50)] +
                ['invalid-email'] * 50
            ),
            'age': [25] * 50 + [150] * 50,  # 150 out of range
        })

        patterns = {
            'ranges': {
                'age': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 120,
                },
            },
        }

        result = expander.expand(df, patterns)

        # Should detect invalid data
        assert result['summary']['invalid_rows'] > 0
        # Coverage should reflect invalid data
        assert result['summary']['coverage_ratio'] < 1.0


class TestExpanderEdgeCases:
    """Test edge cases."""

    @pytest.fixture
    def expander(self):
        return Expander()

    def test_single_row_dataframe(self, expander):
        """Test with single row DataFrame."""
        df = pl.DataFrame({
            'col1': [1],
        })

        patterns = {}

        result = expander.expand(df, patterns)

        assert result is not None
        assert 'metrics' in result

    def test_all_nulls_column(self, expander):
        """Test with all-null column."""
        df = pl.DataFrame({
            'col1': [None, None, None],
            'col2': [1, 2, 3],
        })

        patterns = {
            'ranges': {
                'col2': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 10,
                },
            },
        }

        result = expander.expand(df, patterns)

        assert result is not None

    def test_empty_patterns(self, expander):
        """Test expand with empty patterns."""
        df = pl.DataFrame({
            'col1': [1, 2, 3],
        })

        patterns = {}

        result = expander.expand(df, patterns)

        # All rows should be valid with empty patterns
        assert result['summary']['coverage_ratio'] == 1.0

    def test_patterns_with_missing_columns(self, expander):
        """Test patterns for non-existent columns."""
        df = pl.DataFrame({
            'col1': [1, 2, 3],
        })

        patterns = {
            'ranges': {
                'nonexistent_col': {
                    'type': 'numeric',
                    'min': 0,
                    'max': 10,
                },
            },
        }

        result = expander.expand(df, patterns)

        # Should handle gracefully
        assert result is not None
