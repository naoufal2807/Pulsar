"""
Stage-specific agent implementations for journey execution.

Each stage (SCOUT, EXPLORER, DETECTIVE, ANALYST, ACE) has its own agent
that knows what context it needs and what findings it should extract.
"""

import logging
from typing import Optional, Dict, Any

import polars as pl

from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.llm_connectors import LLMConfig
from pulsar.core.intelligence.prompts import SCOUT_SYSTEM_PROMPT, EXPLORER_SYSTEM_PROMPT, DETECTIVE_SYSTEM_PROMPT, ANALYST_SYSTEM_PROMPT, ACE_SYSTEM_PROMPT
from pulsar.core.intelligence.models import ScoutOutput, ExplorerOutput, DetectiveOutput, AnalystOutput, AceOutput
logger = logging.getLogger(__name__)


class ScoutAgent(ReasoningAgent):
    """
    SCOUT stage: Initial data discovery and quality assessment.

    Responsibilities:
    - Analyze dataset structure
    - Identify key statistics
    - Flag data quality issues
    - Discover initial patterns

    Outputs to shared state:
    - scout.findings: Key discoveries
    - scout.issues: Blocking issues
    - scout.column_stats: Column statistics
    """

    STAGE_NAME = "scout"
    REQUIRED_KEYS = []  # No dependencies - first stage

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
    ):
        super().__init__(
            llm_config=llm_config,
            df=df,
            stage_name=self.STAGE_NAME,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            shared_state=shared_state,
        )

        self.system_prompt = SCOUT_SYSTEM_PROMPT

    def _parse_findings(self, response: str) -> list:
        """Extract findings specific to SCOUT stage."""
        findings = []
        sections = response.split('\n\n')
        for section in sections[:3]:  # Take first 3 sections
            if section.strip():
                findings.append(section.strip()[:200])
        return findings

    def _parse_issues(self, response: str) -> list:
        """Extract data quality issues from response."""
        issues = []
        lines = response.split('\n')
        for line in lines:
            if any(kw in line.lower() for kw in ['issue', 'problem', 'quality', 'missing', 'error', 'critical']):
                issues.append(line.strip())
        return issues[:5]


class ExplorerAgent(ReasoningAgent):
    """
    EXPLORER stage: Detailed data exploration and column analysis.

    Responsibilities:
    - Analyze column types and distributions
    - Explore categorical and numeric patterns
    - Cross-tabulate dimensions
    - Discover relationships

    Inputs from shared state:
    - scout.findings, scout.issues, scout.column_stats

    Outputs to shared state:
    - explorer.columns: Column metadata
    - explorer.distributions: Distribution analysis
    - explorer.categorical_analysis: Category patterns
    """

    STAGE_NAME = "explorer"
    REQUIRED_KEYS = ["scout.findings", "scout.issues", "scout.column_stats"]

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
    ):
        super().__init__(
            llm_config=llm_config,
            df=df,
            stage_name=self.STAGE_NAME,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            shared_state=shared_state,
        )

        self.system_prompt = EXPLORER_SYSTEM_PROMPT


class DetectiveAgent(ReasoningAgent):
    """
    DETECTIVE stage: Deep quality analysis and anomaly detection.

    Responsibilities:
    - Validate data quality assumptions
    - Detect outliers and anomalies
    - Verify data integrity
    - Flag data quality warnings

    Inputs from shared state:
    - scout.*, explorer.*

    Outputs to shared state:
    - detective.quality_verdict: Overall quality assessment
    - detective.validated_findings: Confirmed data issues
    - detective.outliers: Detected anomalies
    """

    STAGE_NAME = "detective"
    REQUIRED_KEYS = [
        "scout.findings",
        "scout.issues",
        "scout.column_stats",
        "explorer.columns",
        "explorer.distributions",
    ]

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
    ):
        super().__init__(
            llm_config=llm_config,
            df=df,
            stage_name=self.STAGE_NAME,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            shared_state=shared_state,
        )

        self.system_prompt = (
            "You are a quality detective validating data integrity and detecting anomalies. "
            "Verify assumptions, check for outliers, and assess overall data quality. "
            "Provide a clear verdict on data readiness."
        )


class AnalystAgent(ReasoningAgent):
    """
    ANALYST stage: Strategic analysis and correlation discovery.

    Responsibilities:
    - Analyze correlations and relationships
    - Identify key drivers and segments
    - Discover strategic opportunities
    - Find high-value insights

    Inputs from shared state:
    - scout.*, explorer.*, detective.*

    Outputs to shared state:
    - analyst.correlations: Key correlations found
    - analyst.segments: Strategic segments
    - analyst.recommendations: Actionable insights
    """

    STAGE_NAME = "analyst"
    REQUIRED_KEYS = [
        "scout.findings",
        "scout.column_stats",
        "explorer.columns",
        "explorer.distributions",
        "detective.quality_verdict",
        "detective.validated_findings",
    ]

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
    ):
        super().__init__(
            llm_config=llm_config,
            df=df,
            stage_name=self.STAGE_NAME,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            shared_state=shared_state,
        )

        self.system_prompt = (
            "You are a strategic analyst finding correlations and opportunities. "
            "Analyze relationships between variables, identify key drivers, and discover segments. "
            "Focus on actionable, high-value insights."
        )


class AceAgent(ReasoningAgent):
    """
    ACE stage: Executive synthesis and recommendations.

    Responsibilities:
    - Synthesize all findings
    - Extract executive summary
    - Provide strategic recommendations
    - Identify risks and opportunities

    Inputs from shared state:
    - All keys from all prior stages

    Outputs to shared state:
    - ace.executive_summary: Summary of findings
    - ace.recommendations: Top recommendations
    - ace.risks: Identified risks
    - ace.opportunities: Strategic opportunities
    """

    STAGE_NAME = "ace"
    REQUIRED_KEYS = []  # Reads all keys via get_by_prefix

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
    ):
        super().__init__(
            llm_config=llm_config,
            df=df,
            stage_name=self.STAGE_NAME,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            shared_state=shared_state,
        )

        self.system_prompt = (
            "You are an executive analyst synthesizing all findings into strategic recommendations. "
            "Provide a clear executive summary, top recommendations, and identified risks/opportunities. "
            "Focus on business value and actionability."
        )

    def _load_from_shared_state(self) -> Dict[str, Any]:
        """Override to load ALL keys for ACE stage."""
        if not self.shared_state:
            return {}

        context = {}
        # Load all keys from all stages
        for prefix in ["scout.", "explorer.", "detective.", "analyst."]:
            all_keys = self.shared_state.get_by_prefix(prefix, stage=self.stage_name)
            context.update(all_keys)

        logger.info(f"ACE loaded {len(context)} keys from all stages")
        return context


# Export all agents
__all__ = [
    "ScoutAgent",
    "ExplorerAgent",
    "DetectiveAgent",
    "AnalystAgent",
    "AceAgent",
]
