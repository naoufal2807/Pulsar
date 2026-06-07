# pulsar/core/intelligence/small_world/expander.py
"""
Expander: Validate and expand patterns to full dataset.

The Expander is the third step in the Small World Framework:
1. Isolator extracts a bounded sample (10%)
2. Learner discovers deep patterns from that sample
3. Expander validates patterns layer-by-layer against growing dataset portions
4. Contextualizer infers business meaning
5. Analyzer generates understanding

The Expander uses a layered approach:
- Layer 1: 10K rows (initial sample) → learn patterns
- Layer 2: 50K rows → validate and update patterns
- Layer 3: 200K rows → refine understanding
- Layer 4: 500K rows → lock in rules
- Layer 5: Full dataset → final validation

At each layer:
1. Validate that patterns hold for new rows
2. Identify contradictions (rows that violate patterns)
3. Update patterns based on new data
4. Track pattern confidence and coverage
"""

from typing import Any, Dict, List, Optional, Tuple, Set
import logging
from dataclasses import dataclass, field
from enum import Enum

import polars as pl

logger = logging.getLogger(__name__)


class ValidationResult(Enum):
    """Result of validating a value against a pattern."""
    VALID = "valid"           # Value matches pattern
    INVALID = "invalid"       # Value violates pattern
    UNKNOWN = "unknown"       # Cannot validate (insufficient info)


@dataclass
class LayerMetrics:
    """Metrics for a validation layer."""
    layer_number: int
    rows_processed: int
    valid_rows: int
    invalid_rows: int
    validation_ratio: float = 0.0
    pattern_updates: int = 0
    contradictions: List[Dict[str, Any]] = field(default_factory=list)

    def __post_init__(self):
        """Calculate validation ratio."""
        if self.rows_processed > 0:
            self.validation_ratio = self.valid_rows / self.rows_processed


class Expander:
    """
    Validate and expand patterns to full dataset.

    Uses layered validation to progressively confirm or refine patterns
    discovered by the Learner.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Expander.

        Args:
            config: Optional configuration with keys:
                - layer_sizes: List of row counts for each layer
                - strict_mode: Enforce all patterns strictly
                - contradiction_threshold: Max % contradictions to accept
        """
        self.config = config or {}
        self.layer_sizes = self.config.get('layer_sizes', [10000, 50000, 200000, 500000])
        self.strict_mode = self.config.get('strict_mode', False)
        self.contradiction_threshold = self.config.get('contradiction_threshold', 0.1)
        self.layers_metrics: List[LayerMetrics] = []
        self.logger = logger

    def expand(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Expand and validate patterns across full dataset.

        Args:
            df: Full dataset to validate against
            patterns: Patterns from Learner

        Returns:
            Dictionary with:
            - 'patterns': Updated patterns
            - 'coverage': Rows that match patterns
            - 'contradictions': Rows that violate patterns
            - 'metrics': Layer-by-layer validation metrics
            - 'quality_score': Overall confidence in patterns
        """
        if df.is_empty():
            self.logger.warning("Empty DataFrame provided to Expander")
            return {
                'patterns': patterns,
                'coverage': df,
                'contradictions': df,
                'metrics': [],
                'quality_score': 0.0,
            }

        self.logger.info(f"Expanding patterns across {df.shape[0]} rows")

        # Reset metrics
        self.layers_metrics = []

        # Process layers progressively
        current_patterns = patterns.copy()
        valid_rows_mask = pl.Series([True] * df.shape[0])
        total_valid = 0
        total_processed = 0

        for layer_num, layer_size in enumerate(self.layer_sizes, 1):
            if layer_size >= df.shape[0]:
                layer_size = df.shape[0]
                actual_layer = df
            else:
                actual_layer = df.head(layer_size)

            # Process this layer
            layer_result = self._process_layer(
                actual_layer,
                current_patterns,
                layer_num,
                layer_size
            )

            # Track metrics
            self.layers_metrics.append(layer_result['metrics'])
            total_valid += layer_result['metrics'].valid_rows
            total_processed += layer_result['metrics'].rows_processed

            # Update patterns based on layer results
            if layer_result['patterns_updated']:
                current_patterns = layer_result['patterns']
                self.logger.debug(f"Layer {layer_num}: Updated patterns")

            # Stop if we've processed the full dataset
            if layer_size >= df.shape[0]:
                break

        # Final validation: mark contradictions
        coverage = self._get_valid_rows(df, current_patterns)
        contradictions = self._get_invalid_rows(df, current_patterns)

        # Calculate overall quality score
        quality_score = (total_valid / total_processed) if total_processed > 0 else 0.0

        return {
            'patterns': current_patterns,
            'coverage': coverage,
            'contradictions': contradictions,
            'metrics': self.layers_metrics,
            'quality_score': round(quality_score, 3),
            'summary': {
                'total_rows': df.shape[0],
                'valid_rows': coverage.shape[0],
                'invalid_rows': contradictions.shape[0],
                'coverage_ratio': round(coverage.shape[0] / df.shape[0], 3) if df.shape[0] > 0 else 0.0,
            }
        }

    def _process_layer(
        self,
        layer_df: pl.DataFrame,
        patterns: Dict[str, Any],
        layer_num: int,
        expected_size: int,
    ) -> Dict[str, Any]:
        """
        Process a single validation layer.

        Args:
            layer_df: DataFrame for this layer
            patterns: Current patterns to validate
            layer_num: Layer number (1-based)
            expected_size: Expected layer size

        Returns:
            Dict with metrics and updated patterns
        """
        self.logger.debug(f"Processing layer {layer_num}: {layer_df.shape[0]} rows")

        # Validate each row against patterns
        valid_count = 0
        invalid_count = 0
        contradictions = []

        for row_idx in range(layer_df.shape[0]):
            row = layer_df.row(row_idx)
            row_valid, violations = self._validate_row(row, layer_df.columns, patterns)

            if row_valid:
                valid_count += 1
            else:
                invalid_count += 1
                contradictions.append({
                    'row_index': row_idx,
                    'violations': violations,
                })

        # Create metrics
        metrics = LayerMetrics(
            layer_number=layer_num,
            rows_processed=layer_df.shape[0],
            valid_rows=valid_count,
            invalid_rows=invalid_count,
            contradictions=contradictions[:10],  # Keep first 10 examples
        )

        # Determine if patterns need updating
        patterns_updated = False
        updated_patterns = patterns.copy()

        # If too many contradictions, try to refine patterns
        contradiction_ratio = invalid_count / layer_df.shape[0] if layer_df.shape[0] > 0 else 0
        if contradiction_ratio > self.contradiction_threshold and not self.strict_mode:
            updated_patterns, patterns_updated = self._refine_patterns(
                layer_df,
                patterns,
                contradictions
            )

        return {
            'metrics': metrics,
            'patterns': updated_patterns,
            'patterns_updated': patterns_updated,
        }

    def _validate_row(
        self,
        row: Tuple,
        columns: List[str],
        patterns: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        """
        Validate a single row against patterns.

        Args:
            row: Row data tuple
            columns: Column names
            patterns: Patterns to validate against

        Returns:
            Tuple of (is_valid, violations)
        """
        violations = []

        # Check format patterns
        if 'formats' in patterns:
            for col_idx, col_name in enumerate(columns):
                if col_name in patterns['formats']:
                    col_formats = patterns['formats'][col_name]
                    col_value = row[col_idx]

                    # Check if value matches any detected format
                    if col_value is not None:
                        value_matches_format = self._check_format(
                            str(col_value),
                            col_formats
                        )
                        if not value_matches_format and col_formats:
                            violations.append(f"{col_name}: format mismatch")

        # Check range patterns (simple bounds check for numeric)
        if 'ranges' in patterns:
            for col_idx, col_name in enumerate(columns):
                if col_name in patterns['ranges']:
                    col_range = patterns['ranges'][col_name]
                    col_value = row[col_idx]

                    if col_value is not None and col_range.get('type') == 'numeric':
                        if 'min' in col_range and col_value < col_range['min']:
                            violations.append(f"{col_name}: below minimum")
                        if 'max' in col_range and col_value > col_range['max']:
                            violations.append(f"{col_name}: above maximum")

        # Row is valid if no violations
        is_valid = len(violations) == 0
        return is_valid, violations

    def _check_format(self, value: str, col_formats: Dict[str, Any]) -> bool:
        """Check if value matches any detected format."""
        if not col_formats:
            return True

        # If any format was detected with high confidence, value should match one
        high_confidence_formats = [
            fmt for fmt, info in col_formats.items()
            if info.get('match_ratio', 0) > 0.8
        ]

        if not high_confidence_formats:
            return True

        # Value should match at least one high-confidence format
        import re

        patterns = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "url": r"^https?://[^\s]+$",
            "phone_us": r"^(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
            "phone_intl": r"^\+[0-9]{1,3}[-.\s]?[0-9]+$",
            "ipv4": r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$",
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "date_iso": r"^\d{4}-\d{2}-\d{2}$",
            "credit_card": r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$",
        }

        for fmt_name in high_confidence_formats:
            pattern = patterns.get(fmt_name)
            if pattern and re.match(pattern, value):
                return True

        return False

    def _refine_patterns(
        self,
        layer_df: pl.DataFrame,
        patterns: Dict[str, Any],
        contradictions: List[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Refine patterns based on contradictions found.

        Args:
            layer_df: Layer data
            patterns: Current patterns
            contradictions: List of contradictions found

        Returns:
            Tuple of (refined_patterns, was_updated)
        """
        refined = patterns.copy()
        was_updated = False

        # For now, refine by relaxing format constraints if too many violations
        if 'formats' in patterns and contradictions:
            # Extract which columns have violations
            violation_cols: Set[str] = set()
            for c in contradictions:
                for violation in c.get('violations', []):
                    col_name = violation.split(':')[0].strip()
                    violation_cols.add(col_name)

            # Reduce confidence for columns with high violation rates
            for col_name in violation_cols:
                if col_name in refined.get('formats', {}):
                    # Mark as less confident
                    for fmt_name in refined['formats'][col_name]:
                        info = refined['formats'][col_name][fmt_name]
                        info['match_ratio'] = max(0.5, info['match_ratio'] * 0.9)

            if violation_cols:
                was_updated = True

        return refined, was_updated

    def _get_valid_rows(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> pl.DataFrame:
        """
        Get rows that match patterns.

        Args:
            df: DataFrame to filter
            patterns: Patterns to validate

        Returns:
            DataFrame with valid rows
        """
        try:
            valid_mask = pl.Series([True] * df.shape[0])

            # Check each row
            for row_idx in range(df.shape[0]):
                row = df.row(row_idx)
                is_valid, _ = self._validate_row(row, df.columns, patterns)
                valid_mask[row_idx] = is_valid

            return df.filter(valid_mask)

        except Exception as e:
            self.logger.debug(f"Error filtering valid rows: {e}")
            return df

    def _get_invalid_rows(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> pl.DataFrame:
        """
        Get rows that violate patterns.

        Args:
            df: DataFrame to filter
            patterns: Patterns to validate

        Returns:
            DataFrame with invalid rows
        """
        try:
            invalid_mask = pl.Series([False] * df.shape[0])

            # Check each row
            for row_idx in range(df.shape[0]):
                row = df.row(row_idx)
                is_valid, _ = self._validate_row(row, df.columns, patterns)
                invalid_mask[row_idx] = not is_valid

            return df.filter(invalid_mask)

        except Exception as e:
            self.logger.debug(f"Error filtering invalid rows: {e}")
            return df

    def validate_layer(
        self,
        layer_df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate a single layer of data.

        Args:
            layer_df: Layer data to validate
            patterns: Patterns to check

        Returns:
            Dict with validation results
        """
        valid_count = 0
        invalid_count = 0
        violations_by_col: Dict[str, int] = {}

        for row_idx in range(layer_df.shape[0]):
            row = layer_df.row(row_idx)
            is_valid, violations = self._validate_row(row, layer_df.columns, patterns)

            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                for violation in violations:
                    col_name = violation.split(':')[0].strip()
                    violations_by_col[col_name] = violations_by_col.get(col_name, 0) + 1

        return {
            'valid_rows': valid_count,
            'invalid_rows': invalid_count,
            'validation_ratio': valid_count / layer_df.shape[0] if layer_df.shape[0] > 0 else 0,
            'violations_by_column': violations_by_col,
        }

    def filter_contradictions(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> pl.DataFrame:
        """
        Remove rows that contradict patterns.

        Args:
            df: DataFrame to filter
            patterns: Patterns to validate

        Returns:
            DataFrame with contradicting rows removed
        """
        self.logger.debug(f"Filtering contradictions from {df.shape[0]} rows")

        cleaned = self._get_valid_rows(df, patterns)

        self.logger.info(
            f"Filtered {df.shape[0] - cleaned.shape[0]} rows; "
            f"kept {cleaned.shape[0]} valid rows"
        )

        return cleaned
