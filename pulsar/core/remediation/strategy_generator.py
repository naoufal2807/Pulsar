# pulsar/core/remediation/strategy_generator.py
"""
Strategy Generator: Determine optimal fix strategies.

Analyzes data quality issues and recommends best fix strategy
based on:
- Issue type (nulls, duplicates, outliers, formats, types)
- Data context (purpose, importance, volume)
- Impact (business, data quality, downstream)
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class FixAction(Enum):
    """Type of fix action to apply."""
    # Null handling
    DELETE_ROWS = "delete_rows"              # Remove rows with nulls
    FILL_MEAN = "fill_mean"                  # Fill with mean/median
    FILL_MODE = "fill_mode"                  # Fill with most common value
    FILL_FORWARD = "fill_forward"            # Forward fill (time series)
    FILL_BACKWARD = "fill_backward"          # Backward fill (time series)
    FILL_CONSTANT = "fill_constant"          # Fill with constant value

    # Duplicate handling
    REMOVE_DUPLICATES = "remove_duplicates"  # Remove exact duplicates
    KEEP_FIRST = "keep_first"                # Keep first occurrence
    KEEP_LAST = "keep_last"                  # Keep last occurrence
    MERGE_DUPLICATES = "merge_duplicates"    # Merge duplicate rows

    # Outlier handling
    CAP_OUTLIERS = "cap_outliers"            # Cap at bounds
    REMOVE_OUTLIERS = "remove_outliers"      # Remove outliers
    TRANSFORM = "transform"                  # Log/sqrt transform

    # Format/Type handling
    STANDARDIZE_FORMAT = "standardize_format"  # Reformat strings
    CONVERT_TYPE = "convert_type"              # Change data type
    REGEX_EXTRACT = "regex_extract"            # Extract using regex

    # Value handling
    NORMALIZE = "normalize"                  # Normalize values
    CATEGORICAL_MAP = "categorical_map"      # Map to valid categories


@dataclass
class FixActionConfig:
    """Configuration for a fix action."""
    action: FixAction
    parameters: Dict[str, Any]
    confidence: float                        # 0-1 confidence in fix
    expected_rows_affected: int
    data_loss_percent: float                 # % of data that would be removed
    reversible: bool                         # Can this fix be undone?
    rationale: str                           # Why this fix is recommended


class StrategyGenerator:
    """
    Generate optimal fix strategies for data quality issues.

    Analyzes the type and severity of issues to recommend
    the best sequence of fix actions.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize strategy generator.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.logger = logger

    def generate_strategy(
        self,
        issue_type: str,
        column_name: str,
        data_info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[FixActionConfig]:
        """
        Generate fix strategy for a data quality issue.

        Args:
            issue_type: Type of issue (null, duplicate, outlier, format, type)
            column_name: Column affected
            data_info: Info about column (dtype, null_count, cardinality, etc.)
            context: Data context (purpose, importance, volume)

        Returns:
            List of recommended fix actions in priority order
        """
        strategies = []

        if issue_type == "null":
            strategies = self._generate_null_strategy(column_name, data_info, context)
        elif issue_type == "duplicate":
            strategies = self._generate_duplicate_strategy(column_name, data_info, context)
        elif issue_type == "outlier":
            strategies = self._generate_outlier_strategy(column_name, data_info, context)
        elif issue_type == "format":
            strategies = self._generate_format_strategy(column_name, data_info, context)
        elif issue_type == "type":
            strategies = self._generate_type_strategy(column_name, data_info, context)

        return strategies

    def _generate_null_strategy(
        self,
        column_name: str,
        data_info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[FixActionConfig]:
        """Generate strategy for handling nulls."""
        strategies = []

        null_ratio = data_info.get('null_ratio', 0)
        dtype = data_info.get('dtype', 'unknown')
        cardinality = data_info.get('cardinality_ratio', 0)
        total_rows = data_info.get('total_rows', 10000)

        # If very few nulls, just delete
        if null_ratio < 0.01:
            strategies.append(FixActionConfig(
                action=FixAction.DELETE_ROWS,
                parameters={'column': column_name},
                confidence=0.95,
                expected_rows_affected=int(total_rows * null_ratio),
                data_loss_percent=null_ratio * 100,
                reversible=False,
                rationale=f"Only {null_ratio*100:.1f}% nulls - safe to remove",
            ))

        # If moderate nulls, try imputation
        elif null_ratio < 0.1:
            if dtype == 'numeric':
                strategies.append(FixActionConfig(
                    action=FixAction.FILL_MEAN,
                    parameters={'column': column_name, 'method': 'median'},
                    confidence=0.8,
                    expected_rows_affected=int(total_rows * null_ratio),
                    data_loss_percent=0,
                    reversible=True,
                    rationale="Fill numeric nulls with median (robust to outliers)",
                ))
            elif cardinality < 0.2:  # Low cardinality categorical
                strategies.append(FixActionConfig(
                    action=FixAction.FILL_MODE,
                    parameters={'column': column_name},
                    confidence=0.7,
                    expected_rows_affected=int(total_rows * null_ratio),
                    data_loss_percent=0,
                    reversible=True,
                    rationale="Fill categorical nulls with mode (most common value)",
                ))

        # If many nulls, might need to drop column
        else:
            strategies.append(FixActionConfig(
                action=FixAction.DELETE_ROWS,
                parameters={'column': column_name},
                confidence=0.6,
                expected_rows_affected=int(total_rows * null_ratio),
                data_loss_percent=null_ratio * 100,
                reversible=False,
                rationale=f"High null ratio ({null_ratio*100:.1f}%) - consider removing rows",
            ))

        return strategies

    def _generate_duplicate_strategy(
        self,
        column_name: str,
        data_info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[FixActionConfig]:
        """Generate strategy for handling duplicates."""
        strategies = []

        duplicate_ratio = data_info.get('duplicate_ratio', 0)
        total_rows = data_info.get('total_rows', 10000)

        # Remove exact duplicates
        if duplicate_ratio > 0:
            strategies.append(FixActionConfig(
                action=FixAction.REMOVE_DUPLICATES,
                parameters={'column': column_name, 'subset': [column_name]},
                confidence=0.95,
                expected_rows_affected=int(total_rows * duplicate_ratio),
                data_loss_percent=duplicate_ratio * 100,
                reversible=False,
                rationale="Remove exact duplicate rows to maintain data integrity",
            ))

        return strategies

    def _generate_outlier_strategy(
        self,
        column_name: str,
        data_info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[FixActionConfig]:
        """Generate strategy for handling outliers."""
        strategies = []

        outlier_count = data_info.get('outlier_count', 0)
        total_rows = data_info.get('total_rows', 10000)
        outlier_percent = (outlier_count / total_rows * 100) if total_rows > 0 else 0

        # Few outliers - remove
        if outlier_percent < 1:
            strategies.append(FixActionConfig(
                action=FixAction.REMOVE_OUTLIERS,
                parameters={'column': column_name, 'method': 'iqr'},
                confidence=0.85,
                expected_rows_affected=outlier_count,
                data_loss_percent=outlier_percent,
                reversible=False,
                rationale=f"Remove {outlier_count} outliers ({outlier_percent:.1f}% of data)",
            ))

        # Many outliers - cap instead
        else:
            strategies.append(FixActionConfig(
                action=FixAction.CAP_OUTLIERS,
                parameters={'column': column_name, 'method': 'iqr'},
                confidence=0.7,
                expected_rows_affected=outlier_count,
                data_loss_percent=0,
                reversible=True,
                rationale=f"Cap {outlier_count} outliers to valid range (preserves data)",
            ))

        return strategies

    def _generate_format_strategy(
        self,
        column_name: str,
        data_info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[FixActionConfig]:
        """Generate strategy for format issues."""
        strategies = []

        detected_format = data_info.get('detected_format', None)
        format_match_ratio = data_info.get('format_match_ratio', 0)
        total_rows = data_info.get('total_rows', 10000)

        # If format detected, standardize
        if detected_format and format_match_ratio > 0.8:
            non_matching = int(total_rows * (1 - format_match_ratio))

            strategies.append(FixActionConfig(
                action=FixAction.STANDARDIZE_FORMAT,
                parameters={'column': column_name, 'format': detected_format},
                confidence=0.9,
                expected_rows_affected=non_matching,
                data_loss_percent=0,
                reversible=True,
                rationale=f"Standardize {detected_format} format for {non_matching} rows",
            ))

        return strategies

    def _generate_type_strategy(
        self,
        column_name: str,
        data_info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> List[FixActionConfig]:
        """Generate strategy for type mismatches."""
        strategies = []

        current_type = data_info.get('dtype', 'unknown')
        target_type = data_info.get('target_dtype', 'numeric')
        conversion_success = data_info.get('conversion_success_ratio', 0)

        if conversion_success > 0.95:
            strategies.append(FixActionConfig(
                action=FixAction.CONVERT_TYPE,
                parameters={
                    'column': column_name,
                    'from_type': current_type,
                    'to_type': target_type,
                },
                confidence=0.9,
                expected_rows_affected=0,
                data_loss_percent=0,
                reversible=True,
                rationale=f"Convert {current_type} to {target_type} ({conversion_success*100:.1f}% success rate)",
            ))

        return strategies

    def rank_strategies(
        self,
        strategies: List[FixActionConfig],
    ) -> List[FixActionConfig]:
        """
        Rank strategies by effectiveness and safety.

        Prioritizes:
        1. High confidence fixes
        2. Reversible operations
        3. Low data loss

        Args:
            strategies: List of fix strategies

        Returns:
            Ranked strategies (best first)
        """
        def score_strategy(s: FixActionConfig) -> float:
            # Confidence (40%)
            conf_score = s.confidence * 0.4

            # Reversibility (30%)
            rev_score = (1.0 if s.reversible else 0.0) * 0.3

            # Data preservation (30%)
            preservation = (1.0 - s.data_loss_percent / 100) * 0.3

            return conf_score + rev_score + preservation

        return sorted(strategies, key=score_strategy, reverse=True)
