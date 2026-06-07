# pulsar/core/prevention/anomaly_gating.py
"""
Anomaly Gating: Prevent cascading failures from anomalies.

Detects anomalies early and gates them to prevent downstream impact.
"""

from typing import Any, Dict, List, Optional
import logging
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class GateAction(Enum):
    """Action to take when anomaly is gated."""
    ALLOW = "allow"                        # Allow through
    WARN = "warn"                          # Warn but allow
    BLOCK = "block"                        # Block data
    QUARANTINE = "quarantine"              # Quarantine for review
    AUTO_FIX = "auto_fix"                  # Automatically fix
    ESCALATE = "escalate"                  # Escalate to team


@dataclass
class AnomalyGate:
    """Gate rule to prevent cascading failures."""
    name: str
    description: str
    enabled: bool

    # Detection
    trigger_condition: str                 # What triggers the gate
    severity_threshold: int                # Min severity to trigger (0-4)

    # Action
    action: GateAction
    action_parameters: Dict[str, Any]

    # Impact
    blocks_downstream: bool                # Does this gate block downstream?
    downstream_components: List[str]       # Components that depend on this

    # Metadata
    created_by: Optional[str] = None
    version: int = 1


class AnomalyGater:
    """
    Gate anomalies to prevent cascading failures.

    Monitors anomalies and takes preventive action before they
    cascade through the data pipeline.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize anomaly gater.

        Args:
            config: Optional configuration
        """
        self.config = config or {}
        self.gates: Dict[str, AnomalyGate] = {}
        self.logger = logger

    def add_gate(self, gate: AnomalyGate):
        """Add a gate rule."""
        if gate.enabled:
            self.gates[gate.name] = gate
            self.logger.info(f"Added gate: {gate.name}")

    def evaluate_anomaly(
        self,
        anomaly_type: str,
        severity: int,
        column_name: str,
    ) -> Optional[GateAction]:
        """
        Evaluate anomaly against gates.

        Args:
            anomaly_type: Type of anomaly
            severity: Severity level (0-4)
            column_name: Column affected

        Returns:
            Gate action to take, or None if no gate triggered
        """
        for gate in self.gates.values():
            if self._matches_gate(gate, anomaly_type, severity, column_name):
                self.logger.warning(
                    f"Gate '{gate.name}' triggered for {anomaly_type} "
                    f"in {column_name} (severity {severity})"
                )
                return gate.action

        return None

    def _matches_gate(
        self,
        gate: AnomalyGate,
        anomaly_type: str,
        severity: int,
        column_name: str,
    ) -> bool:
        """Check if anomaly matches gate trigger condition."""
        # Simplified: check severity threshold
        if severity >= gate.severity_threshold:
            return True
        return False

    def get_critical_gates(self) -> List[AnomalyGate]:
        """Get gates that block downstream components."""
        return [g for g in self.gates.values() if g.blocks_downstream]

    def get_gates_by_severity(self, min_severity: int) -> List[AnomalyGate]:
        """Get gates triggered by min severity."""
        return [g for g in self.gates.values() if g.severity_threshold <= min_severity]
