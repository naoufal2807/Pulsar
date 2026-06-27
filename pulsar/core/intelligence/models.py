# Agents state models

from pydantic import BaseModel, Field
from typing import List, Dict, Any


# Base agent output
class BaseOutput(BaseModel):
    pass


class ColumnStats(BaseModel):
    dtype: str
    missing_count: int
    missing_ratio: float
    unique_count: int


class ScoutOutput(BaseModel):
    dataset_shape: Dict[str, int]  # {"rows": x, "columns": y}

    columns: Dict[str, ColumnStats]

    basic_stats: Dict[str, Dict[str, float]]  # mean, std, min, max

    findings: List[str]  # factual observations only

    issues: List[str]  # missing values, corrupted data, type issues

    
# Explorer state 
class DistributionSummary(BaseModel):
    mean: float | None = None
    std: float | None = None
    skew: float | None = None
    notes: str


class ColumnProfile(BaseModel):
    column_name: str
    type: str
    distribution: DistributionSummary | None
    categorical_levels: List[str] | None


class Relationship(BaseModel):
    source: str
    target: str
    relationship_type: str  # "correlation", "dependency", "grouping"
    strength: float


class ExplorerOutput(BaseModel):
    columns: List[ColumnProfile]

    distributions: Dict[str, DistributionSummary]

    relationships: List[Relationship]

    categorical_analysis: Dict[str, List[str]]
    

# Detective state
class Anomaly(BaseModel):
    column: str
    description: str
    severity: str  # "low", "medium", "high"


class ValidationIssue(BaseModel):
    source: str  # "scout" or "explorer"
    conflict: str
    description: str


class DetectiveOutput(BaseModel):
    data_quality_score: float  # 0-1

    anomalies: List[Anomaly]

    validation_issues: List[ValidationIssue]

    quality_verdict: str  # "trusted", "partial", "unreliable"
    
# Analyst state
class Correlation(BaseModel):
    variables: List[str]
    strength: float
    interpretation: str


class Segment(BaseModel):
    name: str
    definition: str
    characteristics: Dict[str, Any]


class Insight(BaseModel):
    title: str
    description: str
    impact: str  # "low", "medium", "high"


class AnalystOutput(BaseModel):
    correlations: List[Correlation]

    segments: List[Segment]

    insights: List[Insight]

    recommendations: List[str]
    
# Ace state
class ExecutiveSummary(BaseModel):
    overview: str
    key_takeaways: List[str]


class Risk(BaseModel):
    description: str
    severity: str


class Opportunity(BaseModel):
    description: str
    potential_value: str


class AceOutput(BaseModel):
    executive_summary: ExecutiveSummary

    top_recommendations: List[str]

    risks: List[Risk]

    opportunities: List[Opportunity]