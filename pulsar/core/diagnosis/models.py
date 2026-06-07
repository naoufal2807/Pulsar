# pulsar/core/diagnosis/models.py
"""
Pydantic models for Diagnosis layer data structures.

Ensures type safety and validation across anomaly detection,
root cause analysis, impact assessment, and trend analysis.
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from enum import Enum
from datetime import datetime


class AnomalyType(str, Enum):
    """Types of anomalies."""
    OUTLIER = "outlier"
    BEHAVIORAL_SHIFT = "behavioral_shift"
    CONTEXTUAL = "contextual"
    NULL_PATTERN = "null_pattern"
    DUPLICATE = "duplicate"
    FORMAT_VIOLATION = "format_violation"


class Anomaly(BaseModel):
    """Anomaly detected in data."""
    id: str = Field(default_factory=lambda: str(datetime.now().timestamp()))
    anomaly_type: AnomalyType
    column_name: str
    value: Any = None
    severity: int = Field(ge=0, le=4)  # 0-4 scale
    description: str
    rows_affected: int = 1
    detected_at: datetime = Field(default_factory=datetime.now)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        use_enum_values = False


class RootCause(BaseModel):
    """Root cause analysis result."""
    root_cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    explanation: str
    method: str = Field(description="'llm' or 'heuristic'")
    probable_causes: List[str] = Field(default_factory=list)
    supporting_evidence: List[str] = Field(default_factory=list)
    suggested_action: str = ""
    generated_at: datetime = Field(default_factory=datetime.now)


class Impact(BaseModel):
    """Impact assessment."""
    name: str
    impact_type: str  # 'business', 'operational', 'data_quality'
    impact: float = Field(ge=0.0, le=1.0)
    impact_score: float = Field(ge=0.0, le=10.0)
    affected_rows: int = 0
    recovery_effort: int = Field(ge=0, le=10)
    risk_level: str = Field(description="'low', 'medium', 'high', 'critical'")


class Trend(BaseModel):
    """Trend analysis result."""
    anomaly_type: AnomalyType
    column_name: str
    frequency: int
    last_occurrence: Optional[datetime] = None
    predicted_next_occurrence: Optional[datetime] = None
    trend_direction: str = Field(description="'increasing', 'decreasing', 'stable'")
    confidence: float = Field(ge=0.0, le=1.0)
    clustering: bool = False
    affected_rows: List[int] = Field(default_factory=list)


class DiagnosisResult(BaseModel):
    """Complete diagnosis result."""
    dataset_name: str
    total_anomalies: int
    anomalies: List[Anomaly]
    root_causes: Dict[str, RootCause]
    impacts: List[Impact]
    trends: List[Trend]
    summary: str
    generated_at: datetime = Field(default_factory=datetime.now)
    quality_degradation: float = Field(ge=0.0, le=1.0)
