# pulsar/core/diagnosis/root_cause_analyzer.py
"""
Root Cause Analysis with LLM-powered explanations.

Takes detected anomalies and uses LLM to:
1. Explain why anomalies occurred
2. Identify likely root causes
3. Suggest remediation steps
"""

from typing import Any, Dict, List, Optional
import logging

import polars as pl
from .llm_base import LLMProvider, LLMConfig, LLMProviderType, get_llm_provider
from .models import Anomaly, RootCause, AnomalyType

logger = logging.getLogger(__name__)


class RootCauseAnalyzer:
    """
    Analyze root causes of anomalies using LLM.

    Uses pluggable LLM providers (Ollama, OpenAI, Anthropic, etc.)
    to generate intelligent explanations of detected anomalies.
    Falls back to heuristics if LLM unavailable.
    """

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize root cause analyzer.

        Args:
            llm_config: LLM provider configuration (uses Ollama/Gemma by default)
            config: Optional analyzer configuration
        """
        self.config = config or {}
        self.logger = logger

        # Initialize LLM provider
        if llm_config:
            self.llm_provider = get_llm_provider(llm_config)
            self.use_llm = self.llm_provider.health_check()
        else:
            # Default to Ollama/Gemma3:270m
            default_config = LLMConfig(
                provider_type=LLMProviderType.OLLAMA,
                model_name="gemma3:270m",
                base_url="http://localhost:11434",
                temperature=0.5,
                max_tokens=500
            )
            try:
                self.llm_provider = get_llm_provider(default_config)
                self.use_llm = self.llm_provider.health_check()
            except Exception:
                self.use_llm = False
                self.llm_provider = None

        self.logger.info(f"RootCauseAnalyzer initialized. LLM available: {self.use_llm}")

    def analyze(self, df: pl.DataFrame, anomaly: Anomaly) -> RootCause:
        """
        Analyze root cause of an anomaly.

        Args:
            df: DataFrame being analyzed
            anomaly: Anomaly object to analyze

        Returns:
            RootCause object with analysis results
        """
        try:
            if self.use_llm:
                return self._analyze_with_llm(df, anomaly)
            else:
                return self._analyze_heuristic(df, anomaly)
        except Exception as e:
            self.logger.error(f"Root cause analysis failed: {e}")
            return self._analyze_heuristic(df, anomaly)

    def _analyze_with_llm(self, df: pl.DataFrame, anomaly: Anomaly) -> RootCause:
        """Analyze using LLM (Ollama/Gemma3:270m)."""
        try:
            prompt = self._build_analysis_prompt(df, anomaly)
            response = self.llm_provider.generate(prompt)

            return RootCause(
                root_cause=response[:200],
                confidence=0.8,
                explanation=response,
                method="llm",
                probable_causes=[response.split('.')[0]],
                suggested_action=f"Review {anomaly.column_name} for potential {anomaly.anomaly_type.value}"
            )

        except Exception as e:
            self.logger.warning(f"LLM analysis failed, using heuristics: {e}")
            return self._analyze_heuristic(df, anomaly)

    def _analyze_heuristic(self, df: pl.DataFrame, anomaly: Anomaly) -> RootCause:
        """Analyze using rule-based heuristics."""
        probable_causes = []
        explanation = ""

        if anomaly.anomaly_type == AnomalyType.OUTLIER:
            probable_causes = [
                "Data entry error",
                "System malfunction",
                "Genuine outlier event",
                "Sensor/measurement error"
            ]
            explanation = f"Value {anomaly.value} is statistically outside expected range"

        elif anomaly.anomaly_type == AnomalyType.BEHAVIORAL_SHIFT:
            probable_causes = [
                "Distribution change",
                "Sampling bias",
                "Population shift"
            ]
            explanation = "Data distribution has shifted significantly"

        elif anomaly.anomaly_type == AnomalyType.NULL_PATTERN:
            probable_causes = [
                "Missing data collection",
                "System downtime",
                "Incomplete import"
            ]
            explanation = f"High null ratio in {anomaly.column_name}"

        elif anomaly.anomaly_type == AnomalyType.DUPLICATE:
            probable_causes = [
                "Data not deduplicated",
                "Import duplication",
                "ETL error"
            ]
            explanation = f"Low cardinality suggests duplicates in {anomaly.column_name}"

        return RootCause(
            root_cause=probable_causes[0] if probable_causes else "Unknown",
            confidence=0.6,
            explanation=explanation,
            method="heuristic",
            probable_causes=probable_causes,
            suggested_action=f"Investigate {anomaly.anomaly_type.value} in {anomaly.column_name}"
        )

    def _build_analysis_prompt(self, df: pl.DataFrame, anomaly: Anomaly) -> str:
        """Build LLM prompt for analysis."""
        col_data = df[anomaly.column_name]

        prompt = f"""Analyze this data anomaly:

Dataset: {len(df)} rows
Column: {anomaly.column_name}
Anomaly Type: {anomaly.anomaly_type.value}
Severity: {anomaly.severity}/4
Value: {anomaly.value}

Column Stats:
- Mean: {col_data.mean():.2f}
- Median: {col_data.median():.2f}
- Std Dev: {col_data.std():.2f}
- Unique: {col_data.n_unique()}

Provide the most likely root cause in 1-2 sentences."""

        return prompt

    def analyze_batch(self, df: pl.DataFrame, anomalies: List[Anomaly]) -> Dict[str, RootCause]:
        """
        Analyze multiple anomalies.

        Args:
            df: DataFrame being analyzed
            anomalies: List of Anomaly objects

        Returns:
            Dictionary mapping anomaly ID to RootCause
        """
        results = {}
        for anomaly in anomalies:
            try:
                results[anomaly.id] = self.analyze(df, anomaly)
            except Exception as e:
                self.logger.error(f"Failed to analyze anomaly {anomaly.id}: {e}")
                results[anomaly.id] = self._analyze_heuristic(df, anomaly)

        return results
