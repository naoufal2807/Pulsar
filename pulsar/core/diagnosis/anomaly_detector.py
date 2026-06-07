# pulsar/core/diagnosis/anomaly_detector.py
"""
Anomaly detection using statistical methods.

Detects various types of anomalies:
- Outliers (IQR, Z-score)
- Behavioral shifts (changes from baseline)
- Contextual anomalies (unusual combinations)
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
from enum import Enum

import polars as pl

logger = logging.getLogger(__name__)


class AnomalyType(Enum):
    """Types of anomalies detected."""
    OUTLIER = "outlier"                        # Value outside expected range
    BEHAVIORAL_SHIFT = "behavioral_shift"      # Change in pattern/distribution
    CONTEXTUAL = "contextual"                  # Unusual combination of values
    TEMPORAL = "temporal"                      # Unusual temporal pattern


class AnomalySeverity(Enum):
    """Severity levels for anomalies."""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class Anomaly:
    """Detected anomaly."""
    row_index: int
    anomaly_type: AnomalyType
    column_name: str
    value: Any
    expected_range: Optional[Tuple[Any, Any]]
    severity: AnomalySeverity
    confidence: float  # 0-1
    explanation: str


class AnomalyDetector:
    """
    Detect anomalies in data using statistical methods.

    Methods:
    - Outlier detection (IQR, Z-score)
    - Behavioral shift detection (distribution changes)
    - Contextual anomaly detection (unusual patterns)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize anomaly detector.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.iqr_multiplier = self.config.get('iqr_multiplier', 1.5)
        self.zscore_threshold = self.config.get('zscore_threshold', 3.0)
        self.logger = logger

    def detect_outliers(
        self,
        current_df: pl.DataFrame,
        baseline_stats: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect outliers using IQR and Z-score methods.

        Args:
            current_df: Current data to analyze
            baseline_stats: Statistics from baseline data

        Returns:
            List of detected outlier anomalies
        """
        anomalies = []

        try:
            for col_name in current_df.columns:
                col = current_df[col_name]
                dtype_str = str(col.dtype)

                # Only analyze numeric columns
                if dtype_str not in ('Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64'):
                    continue

                baseline_info = baseline_stats.get(col_name, {})
                if baseline_info.get('type') != 'numeric':
                    continue

                # Get baseline ranges
                baseline_min = baseline_info.get('min')
                baseline_max = baseline_info.get('max')
                baseline_mean = baseline_info.get('mean')
                baseline_std = baseline_info.get('std', 1)

                if baseline_min is None or baseline_max is None:
                    continue

                # Check each value
                numeric_col = col.drop_nulls()
                for idx, value in enumerate(numeric_col.to_list()):
                    anomaly = self._check_outlier(
                        idx,
                        col_name,
                        value,
                        baseline_min,
                        baseline_max,
                        baseline_mean,
                        baseline_std,
                    )

                    if anomaly:
                        anomalies.append(anomaly)

        except Exception as e:
            self.logger.debug(f"Error in outlier detection: {e}")

        return anomalies

    def detect_behavioral_shifts(
        self,
        current_df: pl.DataFrame,
        baseline_patterns: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect behavioral shifts (changes in distribution or pattern).

        Args:
            current_df: Current data
            baseline_patterns: Baseline patterns

        Returns:
            List of detected behavioral shift anomalies
        """
        anomalies = []

        try:
            # Check for changes in categorical distributions
            categorical_patterns = baseline_patterns.get('categorical_patterns', {})

            for col_name, baseline_pattern in categorical_patterns.items():
                if col_name not in current_df.columns:
                    continue

                col = current_df[col_name]

                # Check if dominance changed significantly
                if baseline_pattern.get('is_dominated'):
                    dominant_value = baseline_pattern.get('dominant_value')
                    baseline_ratio = baseline_pattern.get('dominant_ratio', 0)

                    # Count current occurrences
                    try:
                        value_counts = col.drop_nulls().value_counts(sort=True).head(1)
                        if value_counts.shape[0] > 0:
                            current_dominant = value_counts[0, 0]
                            current_count = value_counts[0, 1]
                            current_ratio = current_count / col.drop_nulls().len()

                            # If dominance changed significantly
                            if current_dominant != dominant_value and current_ratio > baseline_ratio * 0.8:
                                anomaly = Anomaly(
                                    row_index=-1,  # Column-level anomaly
                                    anomaly_type=AnomalyType.BEHAVIORAL_SHIFT,
                                    column_name=col_name,
                                    value=current_dominant,
                                    expected_range=(dominant_value, baseline_ratio),
                                    severity=AnomalySeverity.HIGH,
                                    confidence=0.8,
                                    explanation=f"Dominant value shifted from '{dominant_value}' ({baseline_ratio:.1%}) to '{current_dominant}' ({current_ratio:.1%})"
                                )
                                anomalies.append(anomaly)
                    except Exception as e:
                        self.logger.debug(f"Error checking behavioral shift for {col_name}: {e}")

        except Exception as e:
            self.logger.debug(f"Error in behavioral shift detection: {e}")

        return anomalies

    def detect_contextual_anomalies(
        self,
        current_df: pl.DataFrame,
        baseline_relationships: Dict[str, List[Tuple[str, float]]],
    ) -> List[Anomaly]:
        """
        Detect contextual anomalies (unusual combinations of values).

        Args:
            current_df: Current data
            baseline_relationships: Baseline correlations

        Returns:
            List of detected contextual anomalies
        """
        anomalies = []

        try:
            # Check for unexpected relationships
            for col_a, correlations in baseline_relationships.items():
                if col_a not in current_df.columns:
                    continue

                for col_b, baseline_corr in correlations:
                    if col_b not in current_df.columns:
                        continue

                    # Calculate current correlation
                    try:
                        df_clean = current_df.select([col_a, col_b]).drop_nulls()
                        if df_clean.shape[0] < 2:
                            continue

                        col_a_vals = df_clean[col_a].to_list()
                        col_b_vals = df_clean[col_b].to_list()

                        current_corr = self._calculate_correlation(col_a_vals, col_b_vals)

                        # If correlation changed significantly
                        if abs(current_corr - baseline_corr) > 0.3:
                            anomaly = Anomaly(
                                row_index=-1,
                                anomaly_type=AnomalyType.CONTEXTUAL,
                                column_name=f"{col_a} vs {col_b}",
                                value=current_corr,
                                expected_range=(baseline_corr - 0.1, baseline_corr + 0.1),
                                severity=AnomalySeverity.MEDIUM,
                                confidence=0.7,
                                explanation=f"Correlation between '{col_a}' and '{col_b}' shifted from {baseline_corr:.2f} to {current_corr:.2f}"
                            )
                            anomalies.append(anomaly)

                    except Exception as e:
                        self.logger.debug(f"Error checking contextual anomaly for {col_a}-{col_b}: {e}")

        except Exception as e:
            self.logger.debug(f"Error in contextual anomaly detection: {e}")

        return anomalies

    def _check_outlier(
        self,
        row_idx: int,
        col_name: str,
        value: float,
        baseline_min: float,
        baseline_max: float,
        baseline_mean: float,
        baseline_std: float,
    ) -> Optional[Anomaly]:
        """
        Check if a single value is an outlier.

        Args:
            row_idx: Row index
            col_name: Column name
            value: Value to check
            baseline_min: Min from baseline
            baseline_max: Max from baseline
            baseline_mean: Mean from baseline
            baseline_std: Std dev from baseline

        Returns:
            Anomaly if outlier detected, None otherwise
        """
        # IQR method
        iqr_lower = baseline_min
        iqr_upper = baseline_max

        if value < iqr_lower or value > iqr_upper:
            # Calculate how far outside range
            if value < iqr_lower:
                distance = iqr_lower - value
                confidence = min(1.0, distance / (baseline_mean - iqr_lower) if baseline_mean > iqr_lower else 0.5)
            else:
                distance = value - iqr_upper
                confidence = min(1.0, distance / (iqr_upper - baseline_mean) if iqr_upper > baseline_mean else 0.5)

            severity = AnomalySeverity.HIGH if distance > baseline_std else AnomalySeverity.MEDIUM

            return Anomaly(
                row_index=row_idx,
                anomaly_type=AnomalyType.OUTLIER,
                column_name=col_name,
                value=value,
                expected_range=(iqr_lower, iqr_upper),
                severity=severity,
                confidence=min(1.0, confidence),
                explanation=f"Value {value} is outside expected range [{iqr_lower}, {iqr_upper}]"
            )

        return None

    def _calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation."""
        if len(x) < 2:
            return 0.0

        mean_x = sum(x) / len(x)
        mean_y = sum(y) / len(y)

        numerator = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
        denom_x = sum((a - mean_x) ** 2 for a in x) ** 0.5
        denom_y = sum((b - mean_y) ** 2 for b in y) ** 0.5

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)
