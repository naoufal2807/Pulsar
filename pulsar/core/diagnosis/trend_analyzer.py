# pulsar/core/diagnosis/trend_analyzer.py
"""
Trend Analysis: Detect patterns in anomalies over time.

Analyzes:
- Anomaly frequency (increasing/decreasing)
- Anomaly clustering (appearing together)
- Anomaly evolution (changing characteristics)
- Predictive signals (likelihood of future anomalies)
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta

from .models import Anomaly, AnomalyType, Trend

logger = logging.getLogger(__name__)


class TrendDirection(Enum):
    """Direction of trend."""
    IMPROVING = "improving"          # Anomalies decreasing
    STABLE = "stable"                # No significant change
    DEGRADING = "degrading"          # Anomalies increasing
    CRITICAL = "critical"            # Rapid increase


@dataclass
class AnomalyTrend:
    """Trend in anomaly occurrence."""
    column_name: str
    anomaly_type: AnomalyType

    # Frequency metrics
    count_last_hour: int
    count_last_day: int
    count_last_week: int

    # Trend analysis
    direction: TrendDirection
    rate_of_change: float  # Anomalies per hour
    acceleration: float    # Change in rate

    # Clustering
    clustering_coefficient: float  # 0-1 (are anomalies clustered?)

    # Prediction
    predicted_next_occurrence_hours: Optional[float]
    confidence: float  # 0-1

    # Summary
    summary: str = ""


class TrendAnalyzer:
    """
    Analyze trends in anomalies to detect patterns and predict future issues.

    Methods:
    - Track anomaly frequency over time
    - Detect clustering and correlation
    - Predict next anomaly occurrence
    - Alert on degrading trends
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize trend analyzer.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.anomaly_history: List[Tuple[datetime, Anomaly]] = []
        self.logger = logger

    def add_anomaly(self, anomaly: Anomaly):
        """
        Add anomaly to history for trend analysis.

        Args:
            anomaly: Detected anomaly
        """
        self.anomaly_history.append((datetime.now(), anomaly))

    def analyze_trends(self) -> List[AnomalyTrend]:
        """
        Analyze trends in anomaly history.

        Returns:
            List of detected trends
        """
        if not self.anomaly_history:
            return []

        # Group by column and type
        anomaly_groups = self._group_anomalies()
        trends = []

        for (col_name, anom_type), anomalies in anomaly_groups.items():
            try:
                trend = self._analyze_group_trend(
                    col_name,
                    anom_type,
                    anomalies,
                )
                trends.append(trend)

            except Exception as e:
                self.logger.debug(f"Error analyzing trend for {col_name}/{anom_type}: {e}")

        return trends

    def _group_anomalies(self) -> Dict[Tuple[str, AnomalyType], List[Tuple[datetime, Anomaly]]]:
        """Group anomalies by column and type."""
        groups = {}

        for timestamp, anomaly in self.anomaly_history:
            key = (anomaly.column_name, anomaly.anomaly_type)
            if key not in groups:
                groups[key] = []
            groups[key].append((timestamp, anomaly))

        return groups

    def _analyze_group_trend(
        self,
        column_name: str,
        anomaly_type: AnomalyType,
        anomalies: List[Tuple[datetime, Anomaly]],
    ) -> AnomalyTrend:
        """Analyze trend for specific column/type combination."""
        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        one_day_ago = now - timedelta(days=1)
        one_week_ago = now - timedelta(weeks=1)

        # Count anomalies in different time windows
        count_last_hour = sum(1 for ts, _ in anomalies if ts >= one_hour_ago)
        count_last_day = sum(1 for ts, _ in anomalies if ts >= one_day_ago)
        count_last_week = sum(1 for ts, _ in anomalies if ts >= one_week_ago)

        # Calculate rate of change
        rate_of_change = self._calculate_rate_of_change(anomalies)
        acceleration = self._calculate_acceleration(anomalies)

        # Determine trend direction
        direction = self._determine_trend_direction(
            count_last_hour,
            count_last_day,
            rate_of_change,
        )

        # Analyze clustering
        clustering_coeff = self._calculate_clustering(anomalies)

        # Predict next occurrence
        predicted_hours, confidence = self._predict_next_occurrence(anomalies)

        # Generate summary
        summary = self._generate_trend_summary(
            column_name,
            direction,
            count_last_day,
            rate_of_change,
        )

        return AnomalyTrend(
            column_name=column_name,
            anomaly_type=anomaly_type,
            count_last_hour=count_last_hour,
            count_last_day=count_last_day,
            count_last_week=count_last_week,
            direction=direction,
            rate_of_change=round(rate_of_change, 3),
            acceleration=round(acceleration, 3),
            clustering_coefficient=round(clustering_coeff, 2),
            predicted_next_occurrence_hours=predicted_hours,
            confidence=round(confidence, 2),
            summary=summary,
        )

    def _calculate_rate_of_change(
        self,
        anomalies: List[Tuple[datetime, Anomaly]],
    ) -> float:
        """Calculate rate of anomaly occurrence."""
        if len(anomalies) < 2:
            return 0.0

        now = datetime.now()
        one_day_ago = now - timedelta(days=1)

        recent = [ts for ts, _ in anomalies if ts >= one_day_ago]
        if not recent:
            return 0.0

        # Anomalies per hour in last day
        hours_elapsed = 24.0
        return len(recent) / hours_elapsed

    def _calculate_acceleration(
        self,
        anomalies: List[Tuple[datetime, Anomaly]],
    ) -> float:
        """Calculate acceleration (change in rate)."""
        if len(anomalies) < 4:
            return 0.0

        now = datetime.now()
        two_days_ago = now - timedelta(days=2)
        one_day_ago = now - timedelta(days=1)

        first_half = [ts for ts, _ in anomalies if two_days_ago <= ts < one_day_ago]
        second_half = [ts for ts, _ in anomalies if ts >= one_day_ago]

        rate_first = len(first_half) / 24.0
        rate_second = len(second_half) / 24.0

        return rate_second - rate_first

    def _determine_trend_direction(
        self,
        count_last_hour: int,
        count_last_day: int,
        rate_of_change: float,
    ) -> TrendDirection:
        """Determine if trend is improving, stable, or degrading."""
        # If many anomalies in recent hour, critical
        if count_last_hour >= 5:
            return TrendDirection.CRITICAL

        # If increasing rate
        if rate_of_change > 0.5:  # More than 1 per 2 hours
            return TrendDirection.DEGRADING

        # If decreasing or stable
        if count_last_day == 0:
            return TrendDirection.IMPROVING
        else:
            return TrendDirection.STABLE

    def _calculate_clustering(
        self,
        anomalies: List[Tuple[datetime, Anomaly]],
    ) -> float:
        """
        Calculate how clustered anomalies are in time.

        0 = spread out, 1 = clustered together
        """
        if len(anomalies) < 2:
            return 0.0

        timestamps = [ts for ts, _ in anomalies]
        timestamps.sort()

        # Calculate gaps between consecutive anomalies
        gaps = []
        for i in range(1, len(timestamps)):
            gap = (timestamps[i] - timestamps[i-1]).total_seconds() / 3600  # In hours
            gaps.append(gap)

        if not gaps:
            return 0.0

        avg_gap = sum(gaps) / len(gaps)
        min_gap = min(gaps)

        # If min gap is much smaller than average, anomalies are clustered
        if avg_gap > 0:
            clustering = 1.0 - (min_gap / avg_gap)
            return max(0, min(1, clustering))
        else:
            return 0.0

    def _predict_next_occurrence(
        self,
        anomalies: List[Tuple[datetime, Anomaly]],
    ) -> Tuple[Optional[float], float]:
        """
        Predict when next anomaly will occur.

        Returns:
            (hours_until_next, confidence)
        """
        if len(anomalies) < 2:
            return None, 0.0

        timestamps = [ts for ts, _ in anomalies]
        timestamps.sort()

        # Average gap between anomalies
        gaps = []
        for i in range(1, len(timestamps)):
            gap = (timestamps[i] - timestamps[i-1]).total_seconds() / 3600  # In hours
            gaps.append(gap)

        if not gaps:
            return None, 0.0

        avg_gap = sum(gaps) / len(gaps)
        last_anomaly = timestamps[-1]
        hours_since = (datetime.now() - last_anomaly).total_seconds() / 3600

        predicted_hours = max(0, avg_gap - hours_since)

        # Confidence based on consistency of gaps
        if avg_gap > 0:
            variance = sum((g - avg_gap) ** 2 for g in gaps) / len(gaps)
            std_dev = variance ** 0.5
            consistency = 1.0 - (std_dev / avg_gap) if std_dev < avg_gap else 0.0
            confidence = max(0, min(1, consistency))
        else:
            confidence = 0.0

        return predicted_hours if predicted_hours > 0 else None, confidence

    def _generate_trend_summary(
        self,
        column_name: str,
        direction: TrendDirection,
        count_last_day: int,
        rate_of_change: float,
    ) -> str:
        """Generate human-readable trend summary."""
        summary = f"'{column_name}' is {direction.value}"

        if count_last_day > 0:
            summary += f" ({count_last_day} anomalies in last 24h)"

        if rate_of_change > 0.5:
            summary += f", increasing at {rate_of_change:.1f} per hour"

        return summary

    def get_critical_trends(self, trends: List[AnomalyTrend]) -> List[AnomalyTrend]:
        """
        Get only critical/degrading trends.

        Args:
            trends: All trends

        Returns:
            Filtered critical trends
        """
        return [
            t for t in trends
            if t.direction in [TrendDirection.CRITICAL, TrendDirection.DEGRADING]
        ]

    def get_predictable_anomalies(self, trends: List[AnomalyTrend]) -> List[AnomalyTrend]:
        """
        Get anomalies with strong prediction confidence.

        Args:
            trends: All trends

        Returns:
            Trends with high confidence prediction
        """
        return [
            t for t in trends
            if t.confidence > 0.7 and t.predicted_next_occurrence_hours is not None
        ]
