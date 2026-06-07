# pulsar/core/prevention/rule_learner.py
"""
Rule Learner: Learn data quality rules to prevent issues.

Analyzes clean data to discover patterns that define "good" data,
then uses these patterns as preventive rules for future data.
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass
from datetime import datetime

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class LearnedRule:
    """Rule learned from data analysis."""
    name: str
    column_name: str
    rule_type: str                         # range, format, cardinality, distribution
    condition: str                         # What makes data valid
    confidence: float                      # 0-1 confidence in rule
    coverage: float                        # % of data this rule applies to

    # Rule definition
    parameters: Dict[str, Any]

    # Metadata
    learned_at: datetime
    learned_from_rows: int                 # Rows used to learn this
    applicability: List[str]               # When this rule should apply

    # Effectiveness
    num_times_triggered: int = 0
    times_prevented_issue: int = 0
    effectiveness: float = 0.0


class RuleLearner:
    """
    Learn data quality rules from clean data.

    Discovers patterns in good data to create preventive rules:
    - Value ranges (min, max, outliers)
    - Format patterns (email, phone, etc.)
    - Cardinality bounds (unique value counts)
    - Distribution characteristics (skewness, kurtosis)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize rule learner.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.learned_rules: Dict[str, LearnedRule] = {}
        self.logger = logger

    def learn_rules(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[LearnedRule]:
        """
        Learn preventive rules from clean data.

        Args:
            df: Clean reference DataFrame
            patterns: Patterns discovered by Learner
            context: Data context

        Returns:
            List of learned rules
        """
        rules = []

        try:
            # Learn range rules for numeric columns
            ranges = patterns.get('ranges', {})
            for col_name, col_info in ranges.items():
                if col_info.get('type') == 'numeric':
                    rule = self._learn_range_rule(col_name, col_info, len(df))
                    if rule:
                        rules.append(rule)
                        self.learned_rules[rule.name] = rule

            # Learn format rules for string columns
            formats = patterns.get('formats', {})
            for col_name, col_formats in formats.items():
                rule = self._learn_format_rule(col_name, col_formats, len(df))
                if rule:
                    rules.append(rule)
                    self.learned_rules[rule.name] = rule

            # Learn cardinality rules
            for col_name, col_info in ranges.items():
                if col_info.get('type') == 'categorical':
                    rule = self._learn_cardinality_rule(col_name, col_info, len(df))
                    if rule:
                        rules.append(rule)
                        self.learned_rules[rule.name] = rule

            # Learn distribution rules for numeric columns
            distributions = patterns.get('distributions', {})
            for col_name, dist_info in distributions.items():
                rule = self._learn_distribution_rule(col_name, dist_info, len(df))
                if rule:
                    rules.append(rule)
                    self.learned_rules[rule.name] = rule

            self.logger.info(f"Learned {len(rules)} preventive rules")

        except Exception as e:
            self.logger.error(f"Error learning rules: {e}")

        return rules

    def _learn_range_rule(
        self,
        column_name: str,
        col_info: Dict[str, Any],
        sample_size: int,
    ) -> Optional[LearnedRule]:
        """Learn range rule for numeric column."""
        try:
            min_val = col_info.get('min')
            max_val = col_info.get('max')
            mean_val = col_info.get('mean')
            std_val = col_info.get('std', 0)

            if min_val is None or max_val is None:
                return None

            # Add buffer for outliers (±2 std devs)
            lower_bound = mean_val - (2 * std_val) if mean_val else min_val
            upper_bound = mean_val + (2 * std_val) if mean_val else max_val

            rule = LearnedRule(
                name=f"range_{column_name}",
                column_name=column_name,
                rule_type="range",
                condition=f"{column_name} BETWEEN {lower_bound} AND {upper_bound}",
                confidence=0.85,
                coverage=0.95,  # Applies to 95% of data
                parameters={
                    'lower_bound': lower_bound,
                    'upper_bound': upper_bound,
                    'mean': mean_val,
                    'std': std_val,
                },
                learned_at=datetime.now(),
                learned_from_rows=sample_size,
                applicability=['data_quality', 'anomaly_detection'],
            )

            return rule

        except Exception as e:
            self.logger.debug(f"Error learning range rule for {column_name}: {e}")
            return None

    def _learn_format_rule(
        self,
        column_name: str,
        col_formats: Dict[str, Any],
        sample_size: int,
    ) -> Optional[LearnedRule]:
        """Learn format rule for string column."""
        try:
            # Pick the most common format
            if not col_formats:
                return None

            best_format = max(
                col_formats.items(),
                key=lambda x: x[1].get('match_ratio', 0)
            )
            format_name, format_info = best_format

            if format_info.get('match_ratio', 0) < 0.7:
                return None

            rule = LearnedRule(
                name=f"format_{column_name}_{format_name}",
                column_name=column_name,
                rule_type="format",
                condition=f"{column_name} MATCHES '{format_name}'",
                confidence=format_info.get('match_ratio', 0),
                coverage=format_info.get('match_ratio', 0),
                parameters={
                    'format': format_name,
                    'examples': format_info.get('examples', []),
                },
                learned_at=datetime.now(),
                learned_from_rows=sample_size,
                applicability=['format_validation'],
            )

            return rule

        except Exception as e:
            self.logger.debug(f"Error learning format rule for {column_name}: {e}")
            return None

    def _learn_cardinality_rule(
        self,
        column_name: str,
        col_info: Dict[str, Any],
        sample_size: int,
    ) -> Optional[LearnedRule]:
        """Learn cardinality rule for categorical column."""
        try:
            cardinality = col_info.get('cardinality_ratio', 0)
            unique_count = col_info.get('unique_count', 0)
            top_values = col_info.get('top_values', [])

            if unique_count == 0:
                return None

            rule = LearnedRule(
                name=f"cardinality_{column_name}",
                column_name=column_name,
                rule_type="cardinality",
                condition=f"{column_name} IN ({', '.join(str(v[0]) for v in top_values[:10])})",
                confidence=0.8,
                coverage=cardinality,
                parameters={
                    'unique_count': unique_count,
                    'cardinality_ratio': cardinality,
                    'top_values': top_values[:5],
                },
                learned_at=datetime.now(),
                learned_from_rows=sample_size,
                applicability=['value_validation'],
            )

            return rule

        except Exception as e:
            self.logger.debug(f"Error learning cardinality rule for {column_name}: {e}")
            return None

    def _learn_distribution_rule(
        self,
        column_name: str,
        dist_info: Dict[str, Any],
        sample_size: int,
    ) -> Optional[LearnedRule]:
        """Learn distribution rule for numeric column."""
        try:
            skewness = dist_info.get('skewness', 0)
            kurtosis = dist_info.get('kurtosis', 0)

            # Classify distribution type
            if abs(skewness) < 0.5:
                dist_type = "normal"
            elif skewness > 0:
                dist_type = "right_skewed"
            else:
                dist_type = "left_skewed"

            rule = LearnedRule(
                name=f"distribution_{column_name}",
                column_name=column_name,
                rule_type="distribution",
                condition=f"{column_name} follows {dist_type} distribution",
                confidence=0.7,
                coverage=1.0,
                parameters={
                    'distribution_type': dist_type,
                    'skewness': skewness,
                    'kurtosis': kurtosis,
                },
                learned_at=datetime.now(),
                learned_from_rows=sample_size,
                applicability=['anomaly_detection', 'outlier_detection'],
            )

            return rule

        except Exception as e:
            self.logger.debug(f"Error learning distribution rule for {column_name}: {e}")
            return None

    def get_rules_for_column(self, column_name: str) -> List[LearnedRule]:
        """Get all learned rules for a column."""
        return [r for r in self.learned_rules.values() if r.column_name == column_name]

    def export_rules(self) -> Dict[str, Dict[str, Any]]:
        """Export learned rules as dictionary."""
        return {
            name: {
                'name': rule.name,
                'column_name': rule.column_name,
                'rule_type': rule.rule_type,
                'condition': rule.condition,
                'confidence': rule.confidence,
                'coverage': rule.coverage,
                'parameters': rule.parameters,
            }
            for name, rule in self.learned_rules.items()
        }

    def update_rule_effectiveness(
        self,
        rule_name: str,
        triggered: bool,
        prevented_issue: bool,
    ):
        """Update rule effectiveness metrics."""
        if rule_name not in self.learned_rules:
            return

        rule = self.learned_rules[rule_name]
        rule.num_times_triggered += 1

        if prevented_issue:
            rule.times_prevented_issue += 1

        # Calculate effectiveness
        if rule.num_times_triggered > 0:
            rule.effectiveness = rule.times_prevented_issue / rule.num_times_triggered
