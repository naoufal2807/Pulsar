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
from dataclasses import dataclass

from .llm_base import LLMProvider, LLMConfig, LLMProviderType, get_llm_provider
from .anomaly_detector import Anomaly, AnomalySeverity

logger = logging.getLogger(__name__)


@dataclass
class RootCauseAnalysis:
    """Root cause analysis result."""
    anomaly: Anomaly
    probable_causes: List[str]
    explanation: str
    remediation_steps: List[str]
    confidence: float


class RootCauseAnalyzer:
    """
    Analyze root causes of anomalies using LLM.

    Uses pluggable LLM providers (Ollama, OpenAI, Anthropic, etc.)
    to generate intelligent explanations of detected anomalies.
    """

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize root cause analyzer.

        Args:
            llm_config: LLM provider configuration
            config: Optional analyzer configuration
        """
        self.config = config or {}
        self.llm = None

        # Initialize LLM provider if config provided
        if llm_config:
            try:
                self.llm = get_llm_provider(llm_config)
                if self.llm.health_check():
                    logger.info(f"LLM provider '{llm_config.model_name}' initialized and healthy")
                else:
                    logger.warning(f"LLM provider '{llm_config.model_name}' health check failed")
                    self.llm = None
            except Exception as e:
                logger.warning(f"Failed to initialize LLM provider: {e}")
                self.llm = None

    def analyze(
        self,
        anomalies: List[Anomaly],
        context: Dict[str, Any],
    ) -> List[RootCauseAnalysis]:
        """
        Analyze root causes for detected anomalies.

        Args:
            anomalies: List of detected anomalies
            context: Data context (patterns, understanding, etc.)

        Returns:
            List of root cause analyses
        """
        analyses = []

        for anomaly in anomalies:
            try:
                # Use LLM if available, otherwise fallback to heuristics
                if self.llm:
                    analysis = self._analyze_with_llm(anomaly, context)
                else:
                    analysis = self._analyze_heuristic(anomaly, context)

                analyses.append(analysis)

            except Exception as e:
                logger.debug(f"Error analyzing anomaly: {e}")
                # Fallback heuristic analysis
                analysis = self._analyze_heuristic(anomaly, context)
                analyses.append(analysis)

        return analyses

    def _analyze_with_llm(
        self,
        anomaly: Anomaly,
        context: Dict[str, Any],
    ) -> RootCauseAnalysis:
        """
        Analyze anomaly using LLM.

        Args:
            anomaly: Detected anomaly
            context: Data context

        Returns:
            Root cause analysis
        """
        try:
            # Build prompt for LLM
            prompt = self._build_analysis_prompt(anomaly, context)

            # Generate explanation using LLM
            explanation = self.llm.generate(prompt)

            # Parse LLM response
            probable_causes = self._extract_causes(explanation)
            remediation_steps = self._extract_remediation(explanation)

            # Calculate confidence based on anomaly
            confidence = min(1.0, anomaly.confidence + 0.2)  # Boost by LLM analysis

            return RootCauseAnalysis(
                anomaly=anomaly,
                probable_causes=probable_causes,
                explanation=explanation[:500],  # Limit explanation length
                remediation_steps=remediation_steps,
                confidence=confidence,
            )

        except Exception as e:
            logger.warning(f"LLM analysis failed: {e}, falling back to heuristics")
            return self._analyze_heuristic(anomaly, context)

    def _analyze_heuristic(
        self,
        anomaly: Anomaly,
        context: Dict[str, Any],
    ) -> RootCauseAnalysis:
        """
        Analyze anomaly using rule-based heuristics.

        Args:
            anomaly: Detected anomaly
            context: Data context

        Returns:
            Root cause analysis
        """
        probable_causes = self._generate_probable_causes(anomaly, context)
        explanation = self._generate_explanation(anomaly, probable_causes)
        remediation_steps = self._generate_remediation(anomaly, probable_causes)

        return RootCauseAnalysis(
            anomaly=anomaly,
            probable_causes=probable_causes,
            explanation=explanation,
            remediation_steps=remediation_steps,
            confidence=anomaly.confidence,
        )

    def _build_analysis_prompt(
        self,
        anomaly: Anomaly,
        context: Dict[str, Any],
    ) -> str:
        """
        Build prompt for LLM analysis.

        Args:
            anomaly: Detected anomaly
            context: Data context

        Returns:
            Prompt for LLM
        """
        data_type = context.get('data_type', 'unknown')
        data_purpose = context.get('data_purpose', 'unknown')

        prompt = f"""Analyze this data anomaly and provide root cause analysis:

ANOMALY DETAILS:
- Type: {anomaly.anomaly_type.value}
- Column: {anomaly.column_name}
- Value: {anomaly.value}
- Expected: {anomaly.expected_range}
- Severity: {anomaly.severity.name}
- Initial Explanation: {anomaly.explanation}

CONTEXT:
- Data Type: {data_type}
- Data Purpose: {data_purpose}
- Confidence: {anomaly.confidence:.1%}

Please provide:
1. Most probable root causes (3-5)
2. Detailed explanation of why this occurred
3. Recommended remediation steps (2-3)

Format as:
ROOT CAUSES:
- [cause 1]
- [cause 2]

EXPLANATION:
[explanation]

REMEDIATION:
- [step 1]
- [step 2]
"""
        return prompt

    def _extract_causes(self, llm_response: str) -> List[str]:
        """
        Extract probable causes from LLM response.

        Args:
            llm_response: LLM generated response

        Returns:
            List of probable causes
        """
        causes = []

        try:
            if 'ROOT CAUSES:' in llm_response:
                causes_section = llm_response.split('ROOT CAUSES:')[1].split('EXPLANATION:')[0]
                for line in causes_section.split('\n'):
                    line = line.strip()
                    if line.startswith('-'):
                        cause = line.lstrip('-').strip()
                        if cause:
                            causes.append(cause)

            return causes if causes else ["Unable to parse causes from LLM response"]

        except Exception as e:
            logger.debug(f"Error extracting causes: {e}")
            return []

    def _extract_remediation(self, llm_response: str) -> List[str]:
        """
        Extract remediation steps from LLM response.

        Args:
            llm_response: LLM generated response

        Returns:
            List of remediation steps
        """
        steps = []

        try:
            if 'REMEDIATION:' in llm_response:
                remediation_section = llm_response.split('REMEDIATION:')[1]
                for line in remediation_section.split('\n'):
                    line = line.strip()
                    if line.startswith('-'):
                        step = line.lstrip('-').strip()
                        if step:
                            steps.append(step)

            return steps[:3]  # Limit to 3 steps

        except Exception as e:
            logger.debug(f"Error extracting remediation: {e}")
            return []

    def _generate_probable_causes(
        self,
        anomaly: Anomaly,
        context: Dict[str, Any],
    ) -> List[str]:
        """
        Generate probable causes using heuristics.

        Args:
            anomaly: Detected anomaly
            context: Data context

        Returns:
            List of probable causes
        """
        causes = []

        # Generic causes based on anomaly type
        if anomaly.anomaly_type.name == 'OUTLIER':
            causes.append("Data entry error or typo")
            causes.append("Temporary system anomaly during data collection")
            causes.append("Legitimate business event (seasonal spike, promotion, etc.)")

        elif anomaly.anomaly_type.name == 'BEHAVIORAL_SHIFT':
            causes.append("Change in underlying process or source system")
            causes.append("External event impacting data generation")
            causes.append("Incomplete or corrupted data batch")

        elif anomaly.anomaly_type.name == 'CONTEXTUAL':
            causes.append("Unexpected correlation in data")
            causes.append("Third-party data enrichment issue")
            causes.append("Regression in upstream data quality")

        return causes

    def _generate_explanation(
        self,
        anomaly: Anomaly,
        probable_causes: List[str],
    ) -> str:
        """
        Generate explanation using heuristics.

        Args:
            anomaly: Detected anomaly
            probable_causes: List of probable causes

        Returns:
            Explanation string
        """
        explanation = f"{anomaly.explanation}\n\nProbable causes: {', '.join(probable_causes)}"
        return explanation

    def _generate_remediation(
        self,
        anomaly: Anomaly,
        probable_causes: List[str],
    ) -> List[str]:
        """
        Generate remediation steps using heuristics.

        Args:
            anomaly: Detected anomaly
            probable_causes: List of probable causes

        Returns:
            List of remediation steps
        """
        steps = []

        # Generic remediation based on severity
        if anomaly.severity == AnomalySeverity.CRITICAL:
            steps.append(f"Immediately investigate row/column: {anomaly.column_name}")
            steps.append("Contact data source team for verification")
            steps.append("Escalate to data quality team")

        elif anomaly.severity == AnomalySeverity.HIGH:
            steps.append(f"Review data quality of '{anomaly.column_name}'")
            steps.append("Verify source system for issues")
            steps.append("Consider data validation rules")

        else:
            steps.append("Log anomaly for trend analysis")
            steps.append("Monitor for recurrence")

        return steps
