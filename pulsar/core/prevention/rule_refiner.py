# pulsar/core/prevention/rule_refiner.py
"""
Rule Refiner: Automatically refine rules based on effectiveness.

Monitors rule performance over time and suggests improvements
based on how well rules prevent quality issues.
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class RuleMetrics:
    """Metrics for a rule's effectiveness."""
    rule_name: str
    rule_type: str

    # Performance
    times_triggered: int = 0
    times_prevented_issue: int = 0
    times_blocked_valid_data: int = 0

    # Effectiveness
    precision: float = 0.0                 # % times it correctly detected issues
    recall: float = 0.0                    # % of issues it caught
    false_positive_rate: float = 0.0       # % times it blocked valid data

    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)


class RuleRefiner:
    """
    Automatically refine rules based on effectiveness.

    Tracks rule performance and suggests:
    - Threshold adjustments (tighten or loosen conditions)
    - Parameter updates (range shifts)
    - Rule retirement (rules that no longer help)
    - New rules (patterns not yet covered)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize rule refiner.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.rule_metrics: Dict[str, RuleMetrics] = {}
        self.refinement_suggestions: List[Dict[str, Any]] = []
        self.logger = logger

    def track_rule_performance(
        self,
        rule_name: str,
        rule_type: str,
        triggered: bool,
        prevented_issue: bool,
        blocked_valid: bool = False,
    ):
        """
        Track a rule's performance.

        Args:
            rule_name: Name of the rule
            rule_type: Type of rule
            triggered: Whether rule was triggered
            prevented_issue: Whether it prevented an issue
            blocked_valid: Whether it incorrectly blocked valid data
        """
        if rule_name not in self.rule_metrics:
            self.rule_metrics[rule_name] = RuleMetrics(
                rule_name=rule_name,
                rule_type=rule_type,
            )

        metrics = self.rule_metrics[rule_name]

        if triggered:
            metrics.times_triggered += 1
            if prevented_issue:
                metrics.times_prevented_issue += 1
            if blocked_valid:
                metrics.times_blocked_valid_data += 1

        self._update_metrics(metrics)

    def _update_metrics(self, metrics: RuleMetrics):
        """Update derived metrics."""
        if metrics.times_triggered > 0:
            # Precision: when rule triggers, how often is it right?
            metrics.precision = metrics.times_prevented_issue / metrics.times_triggered

            # False positive rate: how often does it block valid data?
            metrics.false_positive_rate = metrics.times_blocked_valid_data / metrics.times_triggered

        metrics.last_updated = datetime.now()

    def suggest_refinement(
        self,
        rule_name: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Suggest refinements for a rule based on performance.

        Args:
            rule_name: Name of rule to evaluate

        Returns:
            Suggestion dict or None if no improvement needed
        """
        if rule_name not in self.rule_metrics:
            return None

        metrics = self.rule_metrics[rule_name]

        # Rule never triggered - might be too strict
        if metrics.times_triggered == 0:
            suggestion = {
                'rule_name': rule_name,
                'type': 'loosen_threshold',
                'reason': 'Rule never triggered - may be too strict',
                'confidence': 0.6,
                'action': 'Consider loosening condition or threshold',
            }
            self.refinement_suggestions.append(suggestion)
            return suggestion

        # Low precision - triggers but usually wrong
        if metrics.precision < 0.3:
            suggestion = {
                'rule_name': rule_name,
                'type': 'tighten_threshold',
                'reason': f'Low precision ({metrics.precision:.2%})',
                'confidence': 0.8,
                'action': 'Consider tightening condition to reduce false positives',
            }
            self.refinement_suggestions.append(suggestion)
            return suggestion

        # High false positive rate
        if metrics.false_positive_rate > 0.3:
            suggestion = {
                'rule_name': rule_name,
                'type': 'adjust_parameters',
                'reason': f'High false positive rate ({metrics.false_positive_rate:.2%})',
                'confidence': 0.7,
                'action': 'Review and adjust rule parameters',
            }
            self.refinement_suggestions.append(suggestion)
            return suggestion

        # Rule performing well
        if metrics.precision > 0.8 and metrics.false_positive_rate < 0.1:
            return None

        return None

    def get_rules_needing_refinement(self) -> List[str]:
        """Get rules that could be improved."""
        needing_refinement = []

        for rule_name, metrics in self.rule_metrics.items():
            # Skip rules with insufficient data
            if metrics.times_triggered < 10:
                continue

            # Rule needs refinement if precision is low or false positives are high
            if metrics.precision < 0.5 or metrics.false_positive_rate > 0.2:
                needing_refinement.append(rule_name)

        return needing_refinement

    def recommend_rule_removal(self) -> List[str]:
        """Recommend rules that should be removed."""
        candidates = []

        for rule_name, metrics in self.rule_metrics.items():
            # Rule triggered many times but never prevented an issue
            if metrics.times_triggered > 50 and metrics.times_prevented_issue == 0:
                candidates.append(rule_name)

            # Rule has been disabled/not updated in a long time
            from datetime import timedelta
            if datetime.now() - metrics.last_updated > timedelta(days=30):
                if metrics.times_triggered == 0:
                    candidates.append(rule_name)

        return candidates

    def get_rule_metrics(self, rule_name: str) -> Optional[RuleMetrics]:
        """Get metrics for a specific rule."""
        return self.rule_metrics.get(rule_name)

    def get_all_metrics(self) -> Dict[str, RuleMetrics]:
        """Get all rule metrics."""
        return self.rule_metrics

    def export_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Export metrics for all rules."""
        return {
            rule_name: {
                'rule_type': metrics.rule_type,
                'times_triggered': metrics.times_triggered,
                'times_prevented_issue': metrics.times_prevented_issue,
                'times_blocked_valid_data': metrics.times_blocked_valid_data,
                'precision': metrics.precision,
                'recall': metrics.recall,
                'false_positive_rate': metrics.false_positive_rate,
                'created_at': metrics.created_at.isoformat(),
                'last_updated': metrics.last_updated.isoformat(),
            }
            for rule_name, metrics in self.rule_metrics.items()
        }

    def export_suggestions(self) -> List[Dict[str, Any]]:
        """Export all refinement suggestions."""
        return self.refinement_suggestions
