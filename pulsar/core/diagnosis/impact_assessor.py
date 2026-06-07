# pulsar/core/diagnosis/impact_assessor.py
"""
Impact Assessment: Quantify business impact of anomalies.

Measures:
- Data quality impact (% valid data lost)
- Business impact (revenue, customers affected)
- Operational impact (systems affected, cascade effects)
"""

from typing import Any, Dict, List, Optional
import logging

from .models import Anomaly, AnomalyType, Impact

logger = logging.getLogger(__name__)


class ImpactAssessor:
    """
    Assess business and operational impact of anomalies.

    Quantifies:
    - How much data is affected
    - What systems/users are impacted
    - Cost to fix
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize impact assessor.

        Args:
            config: Optional configuration with cost metrics
        """
        self.config = config or {}
        self.cost_per_hour = self.config.get('cost_per_hour', 500)
        self.revenue_per_row = self.config.get('revenue_per_row', 10)
        self.customers_per_dataset = self.config.get('customers_per_dataset', 100)
        self.logger = logger

    def assess(
        self,
        anomalies: List[Anomaly],
        df_shape: tuple,
    ) -> List[Impact]:
        """
        Assess impact of detected anomalies.

        Args:
            anomalies: List of Anomaly objects
            df_shape: Tuple of (rows, cols) from DataFrame

        Returns:
            List of Impact objects
        """
        impacts = []

        for anomaly in anomalies:
            impact_type = self._determine_impact_type(anomaly)
            affected_ratio = anomaly.rows_affected / df_shape[0]
            risk = self._calculate_risk(anomaly, affected_ratio)

            impact = Impact(
                name=f"{anomaly.column_name}_impact",
                impact_type=impact_type,
                impact=affected_ratio,
                impact_score=risk,
                affected_rows=anomaly.rows_affected,
                recovery_effort=self._estimate_recovery(anomaly),
                risk_level=self._risk_to_level(risk)
            )

            impacts.append(impact)

        self.logger.info(f"Assessed {len(impacts)} impacts")
        return impacts

    def _determine_impact_type(self, anomaly: Anomaly) -> str:
        """Determine impact type based on anomaly type."""
        if anomaly.anomaly_type == AnomalyType.OUTLIER:
            return "data_quality"
        elif anomaly.anomaly_type == AnomalyType.NULL_PATTERN:
            return "data_quality"
        elif anomaly.anomaly_type == AnomalyType.DUPLICATE:
            return "operational"
        elif anomaly.anomaly_type == AnomalyType.BEHAVIORAL_SHIFT:
            return "business"
        else:
            return "operational"

    def _calculate_risk(self, anomaly: Anomaly, ratio: float) -> float:
        """Calculate risk score (0-10)."""
        base_score = anomaly.severity * 2.5  # severity 0-4 -> 0-10
        impact_multiplier = 1 + (ratio * 4)   # Multiply by impact ratio
        return min(base_score * impact_multiplier, 10.0)

    def _estimate_recovery(self, anomaly: Anomaly) -> int:
        """Estimate recovery effort in hours (0-10 scale)."""
        base = {
            AnomalyType.OUTLIER: 2,
            AnomalyType.NULL_PATTERN: 3,
            AnomalyType.DUPLICATE: 4,
            AnomalyType.BEHAVIORAL_SHIFT: 5,
            AnomalyType.FORMAT_VIOLATION: 2,
            AnomalyType.CONTEXTUAL: 3,
        }

        effort = base.get(anomaly.anomaly_type, 3)
        return min(effort + anomaly.severity, 10)

    def _risk_to_level(self, risk: float) -> str:
        """Convert risk score to level."""
        if risk < 2.5:
            return "low"
        elif risk < 5.0:
            return "medium"
        elif risk < 7.5:
            return "high"
        else:
            return "critical"
