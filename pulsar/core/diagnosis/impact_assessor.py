# pulsar/core/diagnosis/impact_assessor.py
"""
Impact Assessment: Quantify business impact of anomalies.

Measures:
- Data quality impact (% valid data lost)
- Business impact (revenue, customers affected)
- Operational impact (systems affected, cascade effects)
- Temporal impact (how long problem persisted)
"""

from typing import Any, Dict, List, Optional
import logging
from dataclasses import dataclass
from enum import Enum

from .anomaly_detector import Anomaly, AnomalySeverity, AnomalyType

logger = logging.getLogger(__name__)


class ImpactScope(Enum):
    """Scope of impact."""
    SINGLE_ROW = "single_row"              # One row affected
    BATCH = "batch"                        # One batch (1-1000 rows)
    COLUMN = "column"                      # Entire column affected
    DATASET = "dataset"                    # Entire dataset affected
    SYSTEM = "system"                      # Multiple datasets/systems


class ImpactDomain(Enum):
    """Domain of impact."""
    DATA_QUALITY = "data_quality"
    ANALYTICS = "analytics"
    ML_MODELS = "ml_models"
    OPERATIONS = "operations"
    BUSINESS = "business"


@dataclass
class Impact:
    """Quantified impact of anomalies."""
    scope: ImpactScope
    domain: ImpactDomain
    severity: AnomalySeverity

    # Metrics
    rows_affected: int
    percentage_affected: float  # 0-1
    estimated_duration_hours: float

    # Business impact
    revenue_impact: Optional[float] = None  # Dollars
    customer_impact: int = 0                # Number of customers

    # Risk metrics
    recovery_effort_hours: float = 1.0
    risk_score: float = 0.5                 # 0-1

    # Description
    summary: str = ""


class ImpactAssessor:
    """
    Assess business and operational impact of anomalies.

    Quantifies:
    - How much data is affected
    - What systems/users are impacted
    - How long problem lasted
    - What it costs to fix
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize impact assessor.

        Args:
            config: Optional configuration with:
                - cost_per_hour: Operational cost per hour
                - revenue_per_row: Average revenue per row
                - customers_per_dataset: Customers using this data
        """
        self.config = config or {}
        self.cost_per_hour = self.config.get('cost_per_hour', 500)  # $/hour
        self.revenue_per_row = self.config.get('revenue_per_row', 10)  # $/row
        self.customers_per_dataset = self.config.get('customers_per_dataset', 100)
        self.logger = logger

    def assess(
        self,
        anomalies: List[Anomaly],
        context: Dict[str, Any],
        dataset_info: Dict[str, Any],
    ) -> List[Impact]:
        """
        Assess impact of detected anomalies.

        Args:
            anomalies: List of detected anomalies
            context: Data context (understanding, characteristics)
            dataset_info: Info about dataset (total rows, purpose, etc.)

        Returns:
            List of impact assessments
        """
        impacts = []

        for anomaly in anomalies:
            try:
                impact = self._assess_single_anomaly(
                    anomaly,
                    context,
                    dataset_info,
                )
                impacts.append(impact)

            except Exception as e:
                self.logger.debug(f"Error assessing impact: {e}")

        return impacts

    def _assess_single_anomaly(
        self,
        anomaly: Anomaly,
        context: Dict[str, Any],
        dataset_info: Dict[str, Any],
    ) -> Impact:
        """
        Assess impact of single anomaly.

        Args:
            anomaly: Detected anomaly
            context: Data context
            dataset_info: Dataset information

        Returns:
            Impact assessment
        """
        # Determine scope
        scope = self._determine_scope(anomaly, dataset_info)

        # Determine affected rows
        rows_affected = self._calculate_rows_affected(scope, dataset_info)
        percentage_affected = (rows_affected / dataset_info.get('total_rows', 1)) if dataset_info.get('total_rows') else 0

        # Determine domain based on data purpose
        domain = self._determine_domain(context)

        # Estimate duration
        duration_hours = self._estimate_duration(anomaly)

        # Calculate business impact
        revenue_impact = self._calculate_revenue_impact(rows_affected, domain)
        customer_impact = self._calculate_customer_impact(percentage_affected)

        # Calculate recovery effort
        recovery_hours = self._estimate_recovery_effort(anomaly, scope)
        recovery_cost = recovery_hours * self.cost_per_hour

        # Calculate risk score
        risk_score = self._calculate_risk_score(
            anomaly.severity,
            scope,
            percentage_affected,
        )

        # Generate summary
        summary = self._generate_summary(
            anomaly,
            scope,
            rows_affected,
            revenue_impact,
        )

        return Impact(
            scope=scope,
            domain=domain,
            severity=anomaly.severity,
            rows_affected=rows_affected,
            percentage_affected=round(percentage_affected, 3),
            estimated_duration_hours=duration_hours,
            revenue_impact=revenue_impact,
            customer_impact=customer_impact,
            recovery_effort_hours=recovery_hours,
            risk_score=round(risk_score, 2),
            summary=summary,
        )

    def _determine_scope(
        self,
        anomaly: Anomaly,
        dataset_info: Dict[str, Any],
    ) -> ImpactScope:
        """Determine scope of impact."""
        if anomaly.row_index >= 0:
            return ImpactScope.SINGLE_ROW

        if anomaly.anomaly_type == AnomalyType.BEHAVIORAL_SHIFT:
            return ImpactScope.COLUMN

        total_rows = dataset_info.get('total_rows', 10000)
        if total_rows > 100000:
            return ImpactScope.DATASET
        else:
            return ImpactScope.BATCH

    def _calculate_rows_affected(
        self,
        scope: ImpactScope,
        dataset_info: Dict[str, Any],
    ) -> int:
        """Calculate number of rows affected."""
        total_rows = dataset_info.get('total_rows', 10000)

        if scope == ImpactScope.SINGLE_ROW:
            return 1
        elif scope == ImpactScope.BATCH:
            return min(1000, total_rows // 10)
        elif scope == ImpactScope.COLUMN:
            return total_rows
        elif scope == ImpactScope.DATASET:
            return total_rows
        else:
            return total_rows

    def _determine_domain(self, context: Dict[str, Any]) -> ImpactDomain:
        """Determine domain of impact based on data purpose."""
        purpose = str(context.get('data_purpose', 'unknown')).lower()

        if 'ml' in purpose:
            return ImpactDomain.ML_MODELS
        elif 'analytics' in purpose or 'bi' in purpose:
            return ImpactDomain.ANALYTICS
        elif 'acquisition' in purpose or 'retention' in purpose or 'monetization' in purpose:
            return ImpactDomain.BUSINESS
        elif 'operations' in purpose or 'monitoring' in purpose:
            return ImpactDomain.OPERATIONS
        else:
            return ImpactDomain.DATA_QUALITY

    def _estimate_duration(self, anomaly: Anomaly) -> float:
        """Estimate how long problem persisted."""
        # Simple heuristic based on severity
        if anomaly.severity == AnomalySeverity.CRITICAL:
            return 24.0  # 24 hours (caught quickly)
        elif anomaly.severity == AnomalySeverity.HIGH:
            return 48.0  # 2 days
        elif anomaly.severity == AnomalySeverity.MEDIUM:
            return 168.0  # 1 week
        else:
            return 720.0  # 1 month

    def _calculate_revenue_impact(
        self,
        rows_affected: int,
        domain: ImpactDomain,
    ) -> Optional[float]:
        """Estimate revenue impact."""
        if domain == ImpactDomain.BUSINESS:
            return float(rows_affected * self.revenue_per_row)
        elif domain == ImpactDomain.ML_MODELS:
            # Indirect impact through reduced ML accuracy
            return float(rows_affected * self.revenue_per_row * 0.1)
        elif domain == ImpactDomain.ANALYTICS:
            # Reduced decision quality
            return float(rows_affected * self.revenue_per_row * 0.05)
        else:
            return None

    def _calculate_customer_impact(self, percentage_affected: float) -> int:
        """Estimate number of customers affected."""
        return int(self.customers_per_dataset * percentage_affected)

    def _estimate_recovery_effort(
        self,
        anomaly: Anomaly,
        scope: ImpactScope,
    ) -> float:
        """Estimate hours needed to recover."""
        base_effort = {
            ImpactScope.SINGLE_ROW: 0.5,
            ImpactScope.BATCH: 2.0,
            ImpactScope.COLUMN: 4.0,
            ImpactScope.DATASET: 8.0,
            ImpactScope.SYSTEM: 24.0,
        }

        effort = base_effort.get(scope, 2.0)

        # Multiply by severity
        severity_multiplier = {
            AnomalySeverity.LOW: 1.0,
            AnomalySeverity.MEDIUM: 1.5,
            AnomalySeverity.HIGH: 2.0,
            AnomalySeverity.CRITICAL: 3.0,
        }

        effort *= severity_multiplier.get(anomaly.severity, 1.0)

        return effort

    def _calculate_risk_score(
        self,
        severity: AnomalySeverity,
        scope: ImpactScope,
        percentage_affected: float,
    ) -> float:
        """Calculate risk score (0-1)."""
        # Base score from severity
        severity_scores = {
            AnomalySeverity.LOW: 0.2,
            AnomalySeverity.MEDIUM: 0.4,
            AnomalySeverity.HIGH: 0.7,
            AnomalySeverity.CRITICAL: 1.0,
        }

        severity_score = severity_scores.get(severity, 0.5)

        # Adjust by scope
        scope_multiplier = {
            ImpactScope.SINGLE_ROW: 0.5,
            ImpactScope.BATCH: 0.8,
            ImpactScope.COLUMN: 1.0,
            ImpactScope.DATASET: 1.2,
            ImpactScope.SYSTEM: 1.5,
        }

        scope_factor = scope_multiplier.get(scope, 1.0)

        # Adjust by percentage affected
        percentage_factor = percentage_affected

        # Combine
        risk = min(1.0, severity_score * scope_factor * percentage_factor)

        return risk

    def _generate_summary(
        self,
        anomaly: Anomaly,
        scope: ImpactScope,
        rows_affected: int,
        revenue_impact: Optional[float],
    ) -> str:
        """Generate human-readable impact summary."""
        summary = f"{anomaly.anomaly_type.value.replace('_', ' ').title()} in '{anomaly.column_name}' "
        summary += f"affects {scope.value.replace('_', ' ')} ({rows_affected} rows)"

        if revenue_impact:
            summary += f" with ${revenue_impact:,.0f} estimated impact"

        return summary

    def prioritize_impacts(self, impacts: List[Impact]) -> List[Impact]:
        """
        Prioritize impacts by risk score.

        Args:
            impacts: List of impact assessments

        Returns:
            Sorted impacts (highest risk first)
        """
        return sorted(impacts, key=lambda x: x.risk_score, reverse=True)
