# pulsar/core/prevention/pattern_monitor.py
"""
Pattern Monitor: Track pattern evolution and detect drift.

Monitors how patterns in data change over time and detects drift
that indicates a shift in data characteristics or quality.
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class PatternDrift:
    """Represents a detected pattern drift."""
    pattern_name: str
    column_name: str
    drift_type: str                        # type_shift, range_shift, distribution_shift
    severity: float                        # 0-1 drift magnitude
    detected_at: datetime
    previous_value: Any
    current_value: Any
    change_magnitude: float                # % change


class PatternMonitor:
    """
    Monitor data patterns for drift over time.

    Tracks pattern evolution:
    - Type shifts (int → float, string → int)
    - Range shifts (values outside historical bounds)
    - Distribution shifts (mean, std dev changes)
    - Cardinality shifts (new values appearing)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize pattern monitor.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.pattern_history: Dict[str, List[Dict[str, Any]]] = {}
        self.drifts: List[PatternDrift] = []
        self.logger = logger

    def record_pattern(
        self,
        column_name: str,
        pattern_name: str,
        pattern_value: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Record a pattern observation for a column.

        Args:
            column_name: Column name
            pattern_name: Type of pattern (e.g., 'range', 'cardinality')
            pattern_value: Current pattern value
            metadata: Additional metadata
        """
        key = f"{column_name}_{pattern_name}"

        if key not in self.pattern_history:
            self.pattern_history[key] = []

        self.pattern_history[key].append({
            'timestamp': datetime.now(),
            'value': pattern_value,
            'metadata': metadata or {},
        })

    def detect_drift(
        self,
        column_name: str,
        pattern_name: str,
        new_value: Any,
        threshold: float = 0.2,
    ) -> Optional[PatternDrift]:
        """
        Detect drift in a pattern.

        Args:
            column_name: Column name
            pattern_name: Pattern type
            new_value: New observed value
            threshold: Drift sensitivity (0-1)

        Returns:
            PatternDrift if detected, else None
        """
        key = f"{column_name}_{pattern_name}"

        if key not in self.pattern_history or len(self.pattern_history[key]) == 0:
            return None

        # Get previous value
        previous_record = self.pattern_history[key][-1]
        previous_value = previous_record['value']

        # Calculate magnitude of change
        magnitude = self._calculate_change_magnitude(previous_value, new_value)

        # Detect drift based on magnitude and threshold
        if magnitude >= threshold:
            drift = PatternDrift(
                pattern_name=pattern_name,
                column_name=column_name,
                drift_type=self._classify_drift(previous_value, new_value),
                severity=min(magnitude, 1.0),
                detected_at=datetime.now(),
                previous_value=previous_value,
                current_value=new_value,
                change_magnitude=magnitude,
            )

            self.drifts.append(drift)
            self.logger.warning(
                f"Drift detected: {column_name}.{pattern_name} "
                f"({drift.drift_type}, severity: {drift.severity:.2f})"
            )

            return drift

        return None

    def _calculate_change_magnitude(self, previous: Any, current: Any) -> float:
        """Calculate magnitude of change between two values."""
        try:
            # Numeric change
            if isinstance(previous, (int, float)) and isinstance(current, (int, float)):
                if previous == 0:
                    return 1.0 if current != 0 else 0.0
                return abs((current - previous) / previous)

            # Range change (tuples)
            if isinstance(previous, (tuple, list)) and isinstance(current, (tuple, list)):
                if len(previous) >= 2 and len(current) >= 2:
                    prev_range = previous[1] - previous[0]
                    curr_range = current[1] - current[0]
                    if prev_range == 0:
                        return 1.0 if curr_range != 0 else 0.0
                    return abs((curr_range - prev_range) / prev_range)

            # Categorical change
            return 0.5 if previous != current else 0.0

        except Exception:
            return 0.0

    def _classify_drift(self, previous: Any, current: Any) -> str:
        """Classify type of drift."""
        if type(previous) != type(current):
            return "type_shift"

        if isinstance(previous, (tuple, list)):
            return "range_shift"

        if isinstance(previous, (int, float)):
            return "distribution_shift"

        return "value_shift"

    def get_recent_drifts(self, hours: int = 24) -> List[PatternDrift]:
        """Get recent drifts within specified hours."""
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=hours)
        return [d for d in self.drifts if d.detected_at >= cutoff]

    def get_critical_drifts(self, severity_threshold: float = 0.7) -> List[PatternDrift]:
        """Get critical drifts (high severity)."""
        return [d for d in self.drifts if d.severity >= severity_threshold]

    def get_pattern_history(self, column_name: str, pattern_name: str) -> List[Dict[str, Any]]:
        """Get pattern history for a column."""
        key = f"{column_name}_{pattern_name}"
        return self.pattern_history.get(key, [])

    def export_drifts(self) -> List[Dict[str, Any]]:
        """Export detected drifts."""
        return [
            {
                'pattern': d.pattern_name,
                'column': d.column_name,
                'drift_type': d.drift_type,
                'severity': d.severity,
                'detected_at': d.detected_at.isoformat(),
                'previous': str(d.previous_value),
                'current': str(d.current_value),
                'magnitude': d.change_magnitude,
            }
            for d in self.drifts
        ]
