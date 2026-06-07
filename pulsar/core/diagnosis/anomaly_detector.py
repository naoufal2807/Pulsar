# pulsar/core/diagnosis/anomaly_detector.py
"""
Anomaly Detector: Find issues in data.

Detects three types of anomalies:
1. Outliers: Values outside statistical bounds
2. Behavioral Shifts: Changes in patterns over time
3. Contextual Anomalies: Values that violate known constraints
"""

from typing import Any, Dict, List, Optional
import logging

import polars as pl
from .models import Anomaly, AnomalyType

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Detect anomalies in data using multiple methods.

    Detects:
    - Outliers: Statistical outliers (IQR, Z-score)
    - Behavioral shifts: Changes in distribution
    - Contextual anomalies: Values violating constraints
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize anomaly detector.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.outlier_threshold = self.config.get('outlier_threshold', 1.5)
        self.shift_threshold = self.config.get('shift_threshold', 0.3)
        self.logger = logger

    def detect_outliers(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect statistical outliers.

        Args:
            df: DataFrame to analyze
            patterns: Patterns from Learner

        Returns:
            List of Anomaly objects
        """
        anomalies = []

        try:
            ranges = patterns.get('ranges', {})

            for col_name, col_info in ranges.items():
                if col_info.get('type') != 'numeric':
                    continue

                col_data = df[col_name]

                # Calculate bounds using IQR
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - self.outlier_threshold * iqr
                upper = q3 + self.outlier_threshold * iqr

                # Find outliers
                outlier_mask = (col_data < lower) | (col_data > upper)
                outlier_indices = df.with_row_index().filter(outlier_mask)['index'].to_list()

                if outlier_indices:
                    for idx in outlier_indices[:5]:  # Limit to 5 per column
                        value = col_data[idx]
                        anomalies.append(Anomaly(
                            anomaly_type=AnomalyType.OUTLIER,
                            column_name=col_name,
                            value=float(value) if value is not None else None,
                            severity=self._calculate_severity(value, lower, upper),
                            description=f"Value {value} outside range [{lower:.2f}, {upper:.2f}]",
                            rows_affected=1,
                            metadata={'index': int(idx), 'bounds': [float(lower), float(upper)]}
                        ))

        except Exception as e:
            self.logger.error(f"Error detecting outliers: {e}")

        return anomalies

    def detect_behavioral_shifts(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect behavioral shifts in data.

        Args:
            df: DataFrame to analyze
            patterns: Patterns from Learner

        Returns:
            List of Anomaly objects
        """
        anomalies = []

        try:
            distributions = patterns.get('distributions', {})

            for col_name, dist_info in distributions.items():
                skewness = dist_info.get('skewness', 0)

                # Detect significant skewness change
                if abs(skewness) > 1.0:
                    anomalies.append(Anomaly(
                        anomaly_type=AnomalyType.BEHAVIORAL_SHIFT,
                        column_name=col_name,
                        severity=min(int(abs(skewness)), 4),
                        description=f"Significant skewness detected (skewness: {skewness:.2f})",
                        metadata={'skewness': skewness}
                    ))

        except Exception as e:
            self.logger.error(f"Error detecting behavioral shifts: {e}")

        return anomalies

    def detect_contextual_anomalies(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect contextual anomalies.

        Args:
            df: DataFrame to analyze
            patterns: Patterns from Learner

        Returns:
            List of Anomaly objects
        """
        anomalies = []

        try:
            # Check for null patterns
            for col_name in df.columns:
                null_count = df[col_name].null_count()
                null_ratio = null_count / len(df)

                if null_ratio > 0.1:  # More than 10% nulls
                    anomalies.append(Anomaly(
                        anomaly_type=AnomalyType.NULL_PATTERN,
                        column_name=col_name,
                        severity=min(int(null_ratio * 4), 4),
                        description=f"High null ratio: {null_ratio:.1%}",
                        rows_affected=null_count,
                        metadata={'null_count': null_count, 'null_ratio': null_ratio}
                    ))

            # Check for duplicates
            for col_name in df.columns:
                if df[col_name].dtype in [str, int]:
                    unique_count = df[col_name].n_unique()
                    cardinality = unique_count / len(df)

                    if cardinality < 0.5:  # Less than 50% unique
                        anomalies.append(Anomaly(
                            anomaly_type=AnomalyType.DUPLICATE,
                            column_name=col_name,
                            severity=2 if cardinality > 0.2 else 3,
                            description=f"Low cardinality: {cardinality:.1%}",
                            metadata={'unique_count': unique_count, 'cardinality': cardinality}
                        ))

        except Exception as e:
            self.logger.error(f"Error detecting contextual anomalies: {e}")

        return anomalies

    def detect_all(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect all types of anomalies.

        Args:
            df: DataFrame to analyze
            patterns: Patterns from Learner

        Returns:
            Combined list of all anomalies
        """
        anomalies = []
        anomalies.extend(self.detect_outliers(df, patterns))
        anomalies.extend(self.detect_behavioral_shifts(df, patterns))
        anomalies.extend(self.detect_contextual_anomalies(df, patterns))

        self.logger.info(f"Detected {len(anomalies)} anomalies")
        return anomalies

    def _calculate_severity(self, value: Any, lower: float, upper: float) -> int:
        """Calculate anomaly severity (0-4)."""
        try:
            if value is None:
                return 1
            value = float(value)
            if value < lower:
                distance = lower - value
            else:
                distance = value - upper

            # Normalize distance to severity
            range_size = upper - lower
            if range_size == 0:
                return 2
            severity = int((distance / range_size) * 4)
            return min(max(severity, 1), 4)
        except:
            return 2
