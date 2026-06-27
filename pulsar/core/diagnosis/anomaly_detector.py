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
from .models import Anomaly, AnomalyType, AnomalySeverity

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
        self.config = config or {}
        self.iqr_multiplier = self.config.get('iqr_multiplier', 1.5)
        self.zscore_threshold = self.config.get('zscore_threshold', 3.0)
        self.shift_threshold = self.config.get('shift_threshold', 0.3)
        self.logger = logger

    def detect_outliers(
        self,
        df: pl.DataFrame,
        patterns: Dict[str, Any],
    ) -> List[Anomaly]:
        """
        Detect statistical outliers using z-score against baseline stats.

        Args:
            df: DataFrame to analyze
            patterns: Dict mapping column name to stats dict with keys:
                      type, mean, std (and optionally min/max)

        Returns:
            List of Anomaly objects
        """
        anomalies = []

        try:
            for col_name, col_info in patterns.items():
                if col_info.get('type') != 'numeric':
                    continue
                if col_name not in df.columns:
                    continue

                mean = col_info.get('mean', 0)
                std = col_info.get('std', 0)
                if std == 0:
                    continue

                col_data = df[col_name].drop_nulls()
                for value in col_data.to_list():
                    zscore = abs(float(value) - mean) / std
                    if zscore > self.zscore_threshold:
                        if zscore > self.zscore_threshold * 1.5:
                            sev = AnomalySeverity.HIGH
                        else:
                            sev = AnomalySeverity.MEDIUM
                        confidence = min(zscore / (self.zscore_threshold * 2), 1.0)
                        anomalies.append(Anomaly(
                            anomaly_type=AnomalyType.OUTLIER,
                            column_name=col_name,
                            value=float(value),
                            severity=int(sev),
                            confidence=confidence,
                            description=(
                                f"Value {value} is {zscore:.1f} std devs "
                                f"from mean ({mean})"
                            ),
                            rows_affected=1,
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
