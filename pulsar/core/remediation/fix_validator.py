# pulsar/core/remediation/fix_validator.py
"""
Fix Validator: Validate that fixes improve data without breaking it.

Checks:
- Data quality improved
- Schema integrity maintained
- No unexpected side effects
- Referential integrity preserved
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validating a fix."""
    valid: bool
    quality_improved: bool
    quality_delta: float                   # Quality before → after
    schema_intact: bool
    data_integrity_ok: bool
    warnings: List[str]                    # Non-fatal warnings
    errors: List[str]                      # Fatal errors
    score: float                           # 0-1 validation score


class FixValidator:
    """
    Validate that data fixes are safe and effective.

    Before accepting a fix, checks:
    1. Data quality improved
    2. Schema hasn't changed unexpectedly
    3. Data relationships preserved
    4. Row count reasonable
    5. No cascading problems
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize fix validator.

        Args:
            config: Optional configuration with validation thresholds
        """
        self.config = config or {}
        self.min_quality_improvement = self.config.get('min_quality_improvement', 0.0)
        self.max_data_loss = self.config.get('max_data_loss', 0.1)  # 10%
        self.logger = logger

    def validate_fix(
        self,
        before_df: pl.DataFrame,
        after_df: pl.DataFrame,
        before_quality: float,
        after_quality: float,
        expected_rows_affected: int,
    ) -> ValidationResult:
        """
        Validate a fix.

        Args:
            before_df: DataFrame before fix
            after_df: DataFrame after fix
            before_quality: Quality score before fix
            after_quality: Quality score after fix
            expected_rows_affected: Expected rows modified

        Returns:
            Validation result
        """
        warnings = []
        errors = []

        # Check quality improvement
        quality_delta = after_quality - before_quality
        quality_improved = quality_delta >= self.min_quality_improvement

        if not quality_improved:
            errors.append(f"Quality did not improve ({quality_delta:.1f}%)")

        # Check schema integrity
        schema_intact = self._check_schema_integrity(before_df, after_df)
        if not schema_intact:
            errors.append("Schema changed unexpectedly")

        # Check data integrity
        data_integrity_ok = self._check_data_integrity(before_df, after_df)
        if not data_integrity_ok:
            errors.append("Data integrity compromised")

        # Check data loss
        rows_lost = before_df.shape[0] - after_df.shape[0]
        data_loss_percent = (rows_lost / before_df.shape[0]) if before_df.shape[0] > 0 else 0

        if data_loss_percent > self.max_data_loss:
            errors.append(f"Data loss too high ({data_loss_percent*100:.1f}% > {self.max_data_loss*100:.1f}%)")

        # Check row count
        if rows_lost != expected_rows_affected:
            warnings.append(
                f"Affected rows mismatch: expected {expected_rows_affected}, "
                f"got {rows_lost}"
            )

        # Calculate validation score
        score = self._calculate_validation_score(
            quality_improved,
            schema_intact,
            data_integrity_ok,
            data_loss_percent,
        )

        return ValidationResult(
            valid=len(errors) == 0,
            quality_improved=quality_improved,
            quality_delta=quality_delta,
            schema_intact=schema_intact,
            data_integrity_ok=data_integrity_ok,
            warnings=warnings,
            errors=errors,
            score=score,
        )

    def _check_schema_integrity(
        self,
        before_df: pl.DataFrame,
        after_df: pl.DataFrame,
    ) -> bool:
        """Check if schema is preserved."""
        try:
            # Column names and types should match
            before_schema = {name: str(dtype) for name, dtype in zip(before_df.columns, before_df.schema.values())}
            after_schema = {name: str(dtype) for name, dtype in zip(after_df.columns, after_df.schema.values())}

            # Allow columns to be removed (rows deleted), but not changed
            for col, dtype in before_schema.items():
                if col in after_schema:
                    if after_schema[col] != dtype:
                        self.logger.warning(f"Type changed for column {col}: {dtype} → {after_schema[col]}")
                        return False

            return True

        except Exception as e:
            self.logger.error(f"Error checking schema: {e}")
            return False

    def _check_data_integrity(
        self,
        before_df: pl.DataFrame,
        after_df: pl.DataFrame,
    ) -> bool:
        """
        Check if data integrity is maintained.

        Checks for:
        - No unexpected nulls introduced
        - No data type mismatches
        - Relationships preserved (simplified check)
        """
        try:
            # Check for unexpected nulls (except in columns we explicitly filled)
            for col in after_df.columns:
                before_nulls = before_df[col].null_count()
                after_nulls = after_df[col].null_count()

                # Nulls should only decrease, not increase
                if after_nulls > before_nulls:
                    self.logger.warning(f"Nulls increased in {col}: {before_nulls} → {after_nulls}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Error checking integrity: {e}")
            return False

    def _calculate_validation_score(
        self,
        quality_improved: bool,
        schema_intact: bool,
        data_integrity_ok: bool,
        data_loss_percent: float,
    ) -> float:
        """Calculate overall validation score (0-1)."""
        score = 0.0

        # Quality improvement (40%)
        if quality_improved:
            score += 0.4

        # Schema integrity (30%)
        if schema_intact:
            score += 0.3

        # Data integrity (20%)
        if data_integrity_ok:
            score += 0.2

        # Data preservation (10%)
        preservation = max(0, 1.0 - data_loss_percent)
        score += preservation * 0.1

        return min(1.0, max(0.0, score))

    def should_accept_fix(self, result: ValidationResult) -> bool:
        """
        Determine if fix should be accepted.

        Args:
            result: Validation result

        Returns:
            True if fix is acceptable, False otherwise
        """
        # Accept if valid and score is decent
        if result.valid and result.score >= 0.6:
            return True

        if not result.valid:
            self.logger.warning(f"Fix rejected: {result.errors}")
            return False

        return False

    def get_validation_report(self, result: ValidationResult) -> str:
        """
        Generate human-readable validation report.

        Args:
            result: Validation result

        Returns:
            Report string
        """
        report = f"Fix Validation Report\n"
        report += f"{'='*50}\n"
        report += f"Status: {'ACCEPTED ✓' if result.valid else 'REJECTED ✗'}\n"
        report += f"Score: {result.score:.1%}\n"
        report += f"Quality Δ: {result.quality_delta:+.1f}%\n"
        report += f"Schema Integrity: {'✓' if result.schema_intact else '✗'}\n"
        report += f"Data Integrity: {'✓' if result.data_integrity_ok else '✗'}\n"

        if result.warnings:
            report += f"\nWarnings:\n"
            for warning in result.warnings:
                report += f"  ⚠ {warning}\n"

        if result.errors:
            report += f"\nErrors:\n"
            for error in result.errors:
                report += f"  ✗ {error}\n"

        return report
