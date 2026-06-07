# tests/prevention/test_prevention.py
"""Tests for prevention layer."""

import pytest
from datetime import datetime, timedelta

from pulsar.core.prevention.rule_learner import RuleLearner, LearnedRule
from pulsar.core.prevention.anomaly_gating import AnomalyGater, AnomalyGate, GateAction
from pulsar.core.prevention.pattern_monitor import PatternMonitor, PatternDrift
from pulsar.core.prevention.rule_refiner import RuleRefiner, RuleMetrics
import polars as pl


class TestRuleLearner:
    """Test rule learning."""

    @pytest.fixture
    def learner(self):
        return RuleLearner()

    @pytest.fixture
    def sample_df(self):
        return pl.DataFrame({
            'age': [25, 30, 35, 40, 45],
            'salary': [50000, 60000, 70000, 80000, 90000],
            'status': ['active', 'inactive', 'active', 'active', 'inactive'],
        })

    @pytest.fixture
    def sample_patterns(self):
        return {
            'ranges': {
                'age': {
                    'type': 'numeric',
                    'min': 25,
                    'max': 45,
                    'mean': 35,
                    'std': 7.07,
                },
                'status': {
                    'type': 'categorical',
                    'unique_count': 2,
                    'cardinality_ratio': 0.4,
                    'top_values': [('active', 0.6), ('inactive', 0.4)],
                },
            },
            'formats': {
                'status': {
                    'status_enum': {
                        'match_ratio': 0.95,
                        'examples': ['active', 'inactive'],
                    }
                }
            },
            'distributions': {
                'age': {
                    'skewness': 0.1,
                    'kurtosis': -1.2,
                }
            },
        }

    def test_learn_range_rule(self, learner, sample_df, sample_patterns):
        """Test learning range rules."""
        rules = learner.learn_rules(sample_df, sample_patterns, {})

        range_rules = [r for r in rules if r.rule_type == 'range']
        assert len(range_rules) > 0

        age_rule = next((r for r in range_rules if r.column_name == 'age'), None)
        assert age_rule is not None
        assert age_rule.confidence > 0.7

    def test_learn_format_rule(self, learner, sample_df, sample_patterns):
        """Test learning format rules."""
        rules = learner.learn_rules(sample_df, sample_patterns, {})

        format_rules = [r for r in rules if r.rule_type == 'format']
        assert len(format_rules) > 0

    def test_learn_cardinality_rule(self, learner, sample_df, sample_patterns):
        """Test learning cardinality rules."""
        rules = learner.learn_rules(sample_df, sample_patterns, {})

        cardinality_rules = [r for r in rules if r.rule_type == 'cardinality']
        assert len(cardinality_rules) > 0

    def test_learn_distribution_rule(self, learner, sample_df, sample_patterns):
        """Test learning distribution rules."""
        rules = learner.learn_rules(sample_df, sample_patterns, {})

        dist_rules = [r for r in rules if r.rule_type == 'distribution']
        assert len(dist_rules) > 0

    def test_get_rules_for_column(self, learner, sample_df, sample_patterns):
        """Test retrieving rules for specific column."""
        learner.learn_rules(sample_df, sample_patterns, {})

        age_rules = learner.get_rules_for_column('age')
        assert len(age_rules) > 0
        assert all(r.column_name == 'age' for r in age_rules)

    def test_export_rules(self, learner, sample_df, sample_patterns):
        """Test exporting rules."""
        learner.learn_rules(sample_df, sample_patterns, {})
        exported = learner.export_rules()

        assert isinstance(exported, dict)
        assert len(exported) > 0

    def test_update_rule_effectiveness(self, learner, sample_df, sample_patterns):
        """Test updating rule effectiveness."""
        rules = learner.learn_rules(sample_df, sample_patterns, {})
        rule_name = rules[0].name

        # Simulate rule triggering
        learner.update_rule_effectiveness(rule_name, triggered=True, prevented_issue=True)
        learner.update_rule_effectiveness(rule_name, triggered=True, prevented_issue=True)
        learner.update_rule_effectiveness(rule_name, triggered=True, prevented_issue=False)

        rule = learner.learned_rules[rule_name]
        assert rule.num_times_triggered == 3
        assert rule.times_prevented_issue == 2
        assert abs(rule.effectiveness - (2/3)) < 0.01


class TestAnomalyGater:
    """Test anomaly gating."""

    @pytest.fixture
    def gater(self):
        return AnomalyGater()

    def test_add_gate(self, gater):
        """Test adding a gate."""
        gate = AnomalyGate(
            name='high_null_rate',
            description='Gate for high null rates',
            enabled=True,
            trigger_condition='null_ratio > 0.1',
            severity_threshold=3,
            action=GateAction.WARN,
            action_parameters={'notify': True},
            blocks_downstream=False,
            downstream_components=[],
        )

        gater.add_gate(gate)
        assert 'high_null_rate' in gater.gates

    def test_evaluate_anomaly_triggers(self, gater):
        """Test anomaly evaluation triggering gate."""
        gate = AnomalyGate(
            name='critical_outlier',
            description='Gate critical outliers',
            enabled=True,
            trigger_condition='severity >= 3',
            severity_threshold=3,
            action=GateAction.BLOCK,
            action_parameters={},
            blocks_downstream=True,
            downstream_components=['report_generation'],
        )

        gater.add_gate(gate)

        # Severity 4 should trigger
        action = gater.evaluate_anomaly('outlier', severity=4, column_name='price')
        assert action == GateAction.BLOCK

    def test_evaluate_anomaly_no_trigger(self, gater):
        """Test anomaly not triggering gate."""
        gate = AnomalyGate(
            name='critical_outlier',
            description='Gate critical outliers',
            enabled=True,
            trigger_condition='severity >= 3',
            severity_threshold=3,
            action=GateAction.BLOCK,
            action_parameters={},
            blocks_downstream=True,
            downstream_components=[],
        )

        gater.add_gate(gate)

        # Severity 2 should not trigger (threshold is 3)
        action = gater.evaluate_anomaly('outlier', severity=2, column_name='price')
        assert action is None

    def test_get_critical_gates(self, gater):
        """Test getting critical gates."""
        gate1 = AnomalyGate(
            name='gate1',
            description='',
            enabled=True,
            trigger_condition='',
            severity_threshold=2,
            action=GateAction.BLOCK,
            action_parameters={},
            blocks_downstream=True,
            downstream_components=['report'],
        )

        gate2 = AnomalyGate(
            name='gate2',
            description='',
            enabled=True,
            trigger_condition='',
            severity_threshold=2,
            action=GateAction.WARN,
            action_parameters={},
            blocks_downstream=False,
            downstream_components=[],
        )

        gater.add_gate(gate1)
        gater.add_gate(gate2)

        critical = gater.get_critical_gates()
        assert len(critical) == 1
        assert critical[0].name == 'gate1'


class TestPatternMonitor:
    """Test pattern monitoring."""

    @pytest.fixture
    def monitor(self):
        return PatternMonitor()

    def test_record_pattern(self, monitor):
        """Test recording patterns."""
        monitor.record_pattern('age', 'range', (25, 65))
        monitor.record_pattern('age', 'range', (25, 70))

        history = monitor.get_pattern_history('age', 'range')
        assert len(history) == 2

    def test_detect_numeric_drift(self, monitor):
        """Test detecting numeric drift."""
        monitor.record_pattern('age', 'mean', 35.0)

        # 50% change
        drift = monitor.detect_drift('age', 'mean', 52.5, threshold=0.4)
        assert drift is not None
        assert drift.severity > 0.4

    def test_no_drift_small_change(self, monitor):
        """Test no drift for small change."""
        monitor.record_pattern('age', 'mean', 35.0)

        # 5% change, threshold is 20%
        drift = monitor.detect_drift('age', 'mean', 36.75, threshold=0.2)
        assert drift is None

    def test_detect_range_drift(self, monitor):
        """Test detecting range shift."""
        monitor.record_pattern('age', 'range', (20, 60))

        # Range doubled (40 to 80)
        drift = monitor.detect_drift('age', 'range', (20, 100), threshold=0.5)
        assert drift is not None
        assert drift.drift_type == 'range_shift'

    def test_get_recent_drifts(self, monitor):
        """Test getting recent drifts."""
        monitor.record_pattern('col', 'val', 1)
        drift1 = monitor.detect_drift('col', 'val', 10, threshold=0.5)

        # Create an old drift by backdating
        old_drift = PatternDrift(
            pattern_name='old',
            column_name='col',
            drift_type='test',
            severity=0.5,
            detected_at=datetime.now() - timedelta(hours=48),
            previous_value=1,
            current_value=10,
            change_magnitude=0.5,
        )
        monitor.drifts.append(old_drift)

        recent = monitor.get_recent_drifts(hours=24)
        assert len(recent) == 1
        assert recent[0] == drift1

    def test_export_drifts(self, monitor):
        """Test exporting drifts."""
        monitor.record_pattern('col', 'val', 1)
        monitor.detect_drift('col', 'val', 10, threshold=0.5)

        exported = monitor.export_drifts()
        assert len(exported) == 1
        assert 'severity' in exported[0]
        assert 'drift_type' in exported[0]


class TestRuleRefiner:
    """Test rule refinement."""

    @pytest.fixture
    def refiner(self):
        return RuleRefiner()

    def test_track_rule_performance(self, refiner):
        """Test tracking rule performance."""
        refiner.track_rule_performance(
            'test_rule',
            'null_check',
            triggered=True,
            prevented_issue=True,
        )

        metrics = refiner.get_rule_metrics('test_rule')
        assert metrics.times_triggered == 1
        assert metrics.times_prevented_issue == 1

    def test_calculate_precision(self, refiner):
        """Test precision calculation."""
        refiner.track_rule_performance('rule1', 'test', True, True)
        refiner.track_rule_performance('rule1', 'test', True, True)
        refiner.track_rule_performance('rule1', 'test', True, False)

        metrics = refiner.get_rule_metrics('rule1')
        assert metrics.times_triggered == 3
        assert abs(metrics.precision - (2/3)) < 0.01

    def test_calculate_false_positive_rate(self, refiner):
        """Test false positive rate calculation."""
        refiner.track_rule_performance('rule1', 'test', True, False, blocked_valid=True)
        refiner.track_rule_performance('rule1', 'test', True, True, blocked_valid=False)

        metrics = refiner.get_rule_metrics('rule1')
        assert abs(metrics.false_positive_rate - 0.5) < 0.01

    def test_suggest_loosen_threshold(self, refiner):
        """Test suggesting to loosen threshold."""
        # Rule never triggered
        suggestion = refiner.suggest_refinement('unused_rule')
        assert suggestion is None

        # Add rule metrics without triggering
        refiner.rule_metrics['unused_rule'] = RuleMetrics(
            rule_name='unused_rule',
            rule_type='test',
            times_triggered=0,
        )

        suggestion = refiner.suggest_refinement('unused_rule')
        assert suggestion is not None
        assert suggestion['type'] == 'loosen_threshold'

    def test_suggest_tighten_threshold(self, refiner):
        """Test suggesting to tighten threshold."""
        for _ in range(10):
            refiner.track_rule_performance('bad_rule', 'test', True, False)

        suggestion = refiner.suggest_refinement('bad_rule')
        assert suggestion is not None
        assert suggestion['type'] == 'tighten_threshold'

    def test_get_rules_needing_refinement(self, refiner):
        """Test finding rules needing refinement."""
        # Low precision rule
        for _ in range(20):
            refiner.track_rule_performance('low_precision', 'test', True, False)

        needing = refiner.get_rules_needing_refinement()
        assert 'low_precision' in needing

    def test_recommend_rule_removal(self, refiner):
        """Test recommending rule removal."""
        # Many triggers, zero issue prevention
        for _ in range(100):
            refiner.track_rule_performance('useless_rule', 'test', True, False)

        candidates = refiner.recommend_rule_removal()
        assert 'useless_rule' in candidates

    def test_export_metrics(self, refiner):
        """Test exporting metrics."""
        refiner.track_rule_performance('rule1', 'test', True, True)

        exported = refiner.export_metrics()
        assert 'rule1' in exported
        assert 'precision' in exported['rule1']

    def test_export_suggestions(self, refiner):
        """Test exporting suggestions."""
        refiner.rule_metrics['test'] = RuleMetrics('test', 'type', times_triggered=0)
        refiner.suggest_refinement('test')

        suggestions = refiner.export_suggestions()
        assert len(suggestions) > 0


class TestPreventionIntegration:
    """Integration tests for prevention layer."""

    def test_full_prevention_pipeline(self):
        """Test complete prevention pipeline."""
        # Create reference data
        df = pl.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'status': ['A', 'B', 'A', 'B', 'A'],
        })

        patterns = {
            'ranges': {
                'value': {
                    'type': 'numeric',
                    'min': 10,
                    'max': 50,
                    'mean': 30,
                    'std': 15,
                }
            },
            'formats': {},
            'distributions': {
                'value': {
                    'skewness': 0.0,
                    'kurtosis': -1.2,
                }
            },
        }

        # Learn rules
        learner = RuleLearner()
        rules = learner.learn_rules(df, patterns, {})
        assert len(rules) > 0

        # Set up gating
        gater = AnomalyGater()
        gate = AnomalyGate(
            name='value_anomaly',
            description='Gate value anomalies',
            enabled=True,
            trigger_condition='severity >= 3',
            severity_threshold=3,
            action=GateAction.BLOCK,
            action_parameters={},
            blocks_downstream=True,
            downstream_components=[],
        )
        gater.add_gate(gate)

        # Monitor patterns
        monitor = PatternMonitor()
        monitor.record_pattern('value', 'mean', 30.0)

        # Simulate data drift
        drift = monitor.detect_drift('value', 'mean', 50.0, threshold=0.3)
        assert drift is not None

        # Refine rules
        refiner = RuleRefiner()
        refiner.track_rule_performance(rules[0].name, 'range', True, True)

        assert len(rules) > 0
        assert len(gater.gates) > 0
        assert len(monitor.drifts) > 0
