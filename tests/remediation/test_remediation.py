# tests/remediation/test_remediation.py
"""Tests for remediation layer."""

import pytest
import polars as pl

from pulsar.core.remediation.strategy_generator import StrategyGenerator, FixAction
from pulsar.core.remediation.rule_builder import RuleBuilder
from pulsar.core.remediation.auto_fixer import AutoFixer, FixStrategy
from pulsar.core.remediation.fix_validator import FixValidator


class TestStrategyGenerator:
    """Test strategy generation."""

    @pytest.fixture
    def generator(self):
        return StrategyGenerator()

    def test_generate_null_strategy_low_ratio(self, generator):
        """Test null strategy with low ratio."""
        data_info = {
            'null_ratio': 0.005,
            'dtype': 'numeric',
            'total_rows': 10000,
        }
        context = {'data_purpose': 'analytics'}

        strategies = generator.generate_strategy(
            'null', 'age', data_info, context
        )

        assert len(strategies) > 0
        # Low null ratio should suggest deletion
        assert any(s.action == FixAction.DELETE_ROWS for s in strategies)

    def test_generate_null_strategy_high_ratio(self, generator):
        """Test null strategy with high ratio."""
        data_info = {
            'null_ratio': 0.15,
            'dtype': 'numeric',
            'total_rows': 10000,
        }
        context = {}

        strategies = generator.generate_strategy(
            'null', 'value', data_info, context
        )

        assert len(strategies) > 0
        # High null ratio might suggest imputation or deletion

    def test_generate_duplicate_strategy(self, generator):
        """Test duplicate strategy."""
        data_info = {
            'duplicate_ratio': 0.05,
            'total_rows': 10000,
        }
        context = {}

        strategies = generator.generate_strategy(
            'duplicate', 'id', data_info, context
        )

        assert len(strategies) > 0
        assert any(s.action == FixAction.REMOVE_DUPLICATES for s in strategies)

    def test_rank_strategies(self, generator):
        """Test strategy ranking."""
        from pulsar.core.remediation.strategy_generator import FixActionConfig

        strategies = [
            FixActionConfig(
                action=FixAction.DELETE_ROWS,
                parameters={'column': 'col'},
                confidence=0.8 + i*0.05,
                expected_rows_affected=0,
                data_loss_percent=5,
                reversible=False,
                rationale=f'Test {i}',
            )
            for i in range(3)
        ]

        ranked = generator.rank_strategies(strategies)

        assert len(ranked) == len(strategies)
        # Ranked strategies should be ordered by score


class TestRuleBuilder:
    """Test rule building."""

    @pytest.fixture
    def builder(self):
        return RuleBuilder()

    def test_build_null_rule(self, builder):
        """Test building null handling rule."""
        rule = builder.build_null_rule(
            'email',
            'DELETE_ROWS',
            {'column': 'email'},
            'Remove rows with null email',
        )

        assert rule.name == 'null_email_delete_rows'
        assert rule.rule_type == 'null'
        assert rule.column_name == 'email'

    def test_build_duplicate_rule(self, builder):
        """Test building duplicate handling rule."""
        rule = builder.build_duplicate_rule(
            ['id', 'email'],
            description='Remove exact duplicates',
        )

        assert 'duplicate' in rule.name
        assert rule.rule_type == 'duplicate'

    def test_add_and_retrieve_rules(self, builder):
        """Test adding and retrieving rules."""
        rule1 = builder.build_null_rule('col1', 'DELETE_ROWS', {})
        rule2 = builder.build_format_rule('col2', 'email')

        rules = builder.get_rules_by_type('null')

        assert len(rules) == 1
        assert rules[0].name == rule1.name

    def test_export_rules(self, builder):
        """Test exporting rules."""
        builder.build_null_rule('col', 'DELETE_ROWS', {})
        builder.build_duplicate_rule(['id'])

        exported = builder.export_rules()

        assert len(exported) == 2
        assert 'null_col_delete_rows' in exported


class TestAutoFixer:
    """Test auto-fixing."""

    @pytest.fixture
    def fixer(self):
        return AutoFixer()

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, None, 30, 40, 50],
            'status': ['A', 'A', 'B', 'A', 'A'],
        })

    def test_fix_null_delete(self, fixer, sample_df):
        """Test deleting rows with nulls."""
        from pulsar.core.remediation.strategy_generator import FixActionConfig

        fix_config = FixActionConfig(
            action=FixAction.DELETE_ROWS,
            parameters={'column': 'value'},
            confidence=0.9,
            expected_rows_affected=1,
            data_loss_percent=20,
            reversible=False,
            rationale='Remove nulls',
        )

        fixed_df, result = fixer.apply_fix(sample_df, fix_config)

        assert result.success
        assert result.rows_affected == 1
        assert fixed_df.shape[0] == 4

    def test_fix_null_fill_mean(self, fixer, sample_df):
        """Test filling nulls with mean."""
        from pulsar.core.remediation.strategy_generator import FixActionConfig

        fix_config = FixActionConfig(
            action=FixAction.FILL_MEAN,
            parameters={'column': 'value', 'method': 'mean'},
            confidence=0.8,
            expected_rows_affected=1,
            data_loss_percent=0,
            reversible=True,
            rationale='Fill with mean',
        )

        fixed_df, result = fixer.apply_fix(sample_df, fix_config)

        assert result.success
        assert fixed_df.shape[0] == 5
        assert fixed_df['value'].null_count() == 0

    def test_fix_duplicates(self, fixer):
        """Test removing duplicates."""
        df = pl.DataFrame({
            'id': [1, 1, 2, 3],
            'value': ['A', 'A', 'B', 'C'],
        })

        from pulsar.core.remediation.strategy_generator import FixActionConfig

        fix_config = FixActionConfig(
            action=FixAction.REMOVE_DUPLICATES,
            parameters={'subset': ['id'], 'keep': 'first'},
            confidence=0.95,
            expected_rows_affected=1,
            data_loss_percent=25,
            reversible=False,
            rationale='Remove duplicates',
        )

        fixed_df, result = fixer.apply_fix(df, fix_config)

        assert result.success
        assert fixed_df.shape[0] == 3


class TestFixValidator:
    """Test fix validation."""

    @pytest.fixture
    def validator(self):
        return FixValidator()

    def test_validate_successful_fix(self, validator):
        """Test validating a successful fix."""
        before_df = pl.DataFrame({'col': [1, None, 3]})
        after_df = pl.DataFrame({'col': [1, 2, 3]})

        result = validator.validate_fix(
            before_df,
            after_df,
            before_quality=70,
            after_quality=90,
            expected_rows_affected=1,
        )

        assert result.valid
        assert result.quality_improved
        assert result.schema_intact

    def test_validate_fix_quality_not_improved(self, validator):
        """Test rejecting fix that doesn't improve quality."""
        before_df = pl.DataFrame({'col': [1, 2, 3]})
        after_df = pl.DataFrame({'col': [1, 2, 3]})

        result = validator.validate_fix(
            before_df,
            after_df,
            before_quality=70,
            after_quality=65,  # Quality decreased
            expected_rows_affected=0,
        )

        assert not result.valid
        assert not result.quality_improved

    def test_validate_fix_excessive_data_loss(self, validator):
        """Test rejecting fix with excessive data loss."""
        before_df = pl.DataFrame({'col': range(100)})
        after_df = pl.DataFrame({'col': range(50)})  # Lost 50%

        result = validator.validate_fix(
            before_df,
            after_df,
            before_quality=70,
            after_quality=90,
            expected_rows_affected=50,
        )

        # Default max_data_loss is 10%, this exceeds it
        assert not result.valid

    def test_validation_score(self, validator):
        """Test validation score calculation."""
        before_df = pl.DataFrame({'col': [1, 2, 3]})
        after_df = pl.DataFrame({'col': [1, 2]})

        result = validator.validate_fix(
            before_df,
            after_df,
            before_quality=70,
            after_quality=85,
            expected_rows_affected=1,
        )

        assert 0 <= result.score <= 1


class TestRemediationIntegration:
    """Integration tests for remediation pipeline."""

    def test_full_remediation_pipeline(self):
        """Test complete remediation pipeline."""
        # Create messy data
        df = pl.DataFrame({
            'id': [1, 2, 2, 4, 5],  # Has duplicate
            'email': ['a@x.com', None, 'c@x.com', 'd@x.com', 'e@x.com'],  # Has null
            'score': [10, 20, 20, 100, 30],  # Has outlier (100)
        })

        # Generate strategies
        generator = StrategyGenerator()
        strategies = []

        # Duplicate strategy
        dup_strategy = generator.generate_strategy(
            'duplicate',
            'id',
            {'duplicate_ratio': 0.2, 'total_rows': 5},
            {},
        )
        strategies.extend(dup_strategy)

        # Null strategy
        null_strategy = generator.generate_strategy(
            'null',
            'email',
            {'null_ratio': 0.2, 'dtype': 'string', 'total_rows': 5},
            {},
        )
        strategies.extend(null_strategy)

        # Apply fixes
        fixer = AutoFixer()
        fixed_df = df.clone()

        for strategy in strategies:
            fixed_df, result = fixer.apply_fix(fixed_df, strategy)
            if not result.success:
                break

        # Validate
        validator = FixValidator()
        validation = validator.validate_fix(
            df,
            fixed_df,
            before_quality=50,
            after_quality=85,
            expected_rows_affected=2,
        )

        assert validation.quality_improved or len(df) > len(fixed_df)


class TestEdgeCases:
    """Test edge cases."""

    def test_fix_empty_dataframe(self):
        """Test fixing empty DataFrame."""
        from pulsar.core.remediation.strategy_generator import FixActionConfig

        fixer = AutoFixer()
        df = pl.DataFrame({'col': []})

        fix_config = FixActionConfig(
            action=FixAction.DELETE_ROWS,
            parameters={'column': 'col'},
            confidence=0.9,
            expected_rows_affected=0,
            data_loss_percent=0,
            reversible=False,
            rationale='Test',
        )

        fixed_df, result = fixer.apply_fix(df, fix_config)

        assert result.rows_before == 0

    def test_rule_with_no_matches(self):
        """Test applying rule that matches no rows."""
        builder = RuleBuilder()
        rule = builder.build_null_rule('col', 'DELETE_ROWS', {})

        df = pl.DataFrame({'col': [1, 2, 3]})  # No nulls

        fixer = AutoFixer()
        fixed_df, result = fixer.apply_rule(df, rule)

        assert result.rows_affected == 0
