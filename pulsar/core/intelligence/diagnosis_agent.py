"""DiagnosisAgent: Problem-focused issue detection and analysis."""

from typing import Any, Dict, List, Optional
import logging
import json

import polars as pl

from pulsar.core.intelligence.agent_base import Agent, Message
from pulsar.core.llm_connectors import LLMConfig
from pulsar.core.intelligence.function_calls import FunctionCallParser

logger = logging.getLogger(__name__)


class DiagnosisAgent(Agent):
    """
    Diagnostic analysis agent for issue detection and root cause analysis.

    Memory strategy: Problem-focused history (stores issue tracking)
    Tool behavior: Iterative (up to 2 iterations for root cause analysis)
    Typical use: Finding issues, diagnosing problems, assessing impact

    Ideal for:
    - Anomaly detection
    - Data quality issues
    - Root cause analysis
    - Impact assessment
    """

    def _init_memory(self) -> None:
        """Initialize problem-focused memory."""
        self.memory: List[Message] = []
        self.issues: Dict[str, Any] = {}  # Track identified issues
        logger.debug(f"{self.__class__.__name__} initialized with problem-focused memory")

    def think(self, question: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Diagnose problems with iterative analysis (max 2 iterations).

        Args:
            question: The problem to investigate
            context: Optional context dictionary

        Returns:
            Diagnostic analysis with root causes and impact
        """
        if not self.provider_available:
            return self._fallback_response(question)

        try:
            logger.debug(f"DiagnosisAgent.think() called: {question[:80]}...")

            # Add user question to memory
            self._add_to_memory("user", question)

            # Build prompt with problem context
            prompt_parts = [self.system_prompt]

            # Add previous issues if investigating further
            if self.memory and len(self.memory) > 1:
                prompt_parts.append("\nPrevious findings:")
                for msg in self.memory[-2:]:
                    prompt_parts.append(f"{msg.role.upper()}: {msg.content[:200]}...")

            # Add context
            if context:
                prompt_parts.append("\nContext:")
                for key, value in context.items():
                    prompt_parts.append(f"- {key}: {value}")

            # Add question
            prompt_parts.append(f"\nProblem to diagnose: {question}")

            prompt = "\n".join(prompt_parts)

            # First diagnostic pass
            logger.debug("Calling LLM for initial diagnosis...")
            response = self.llm_provider.generate(prompt)

            # Check for tool calls (anomalies, data quality checks)
            has_calls = FunctionCallParser.response_has_function_calls(response)

            if has_calls and self.tool_registry:
                logger.info("Tool calls detected - executing diagnostic tools")
                tool_results = self._execute_function_calls(response)

                if tool_results:
                    # Second pass: analyze tool results for root causes
                    current_question = f"Tool results from diagnostic checks:\n{json.dumps(tool_results, default=str)}\n\nBased on these findings, what is the root cause and impact?"
                    logger.debug("Calling LLM for root cause analysis...")
                    response = self.llm_provider.generate(prompt + "\n\n" + current_question)

                    # Store findings
                    self._add_to_memory("assistant", response)
                    return response

            # Single response if no tools called
            self._add_to_memory("assistant", response)
            return response

        except Exception as e:
            logger.error(f"DiagnosisAgent thinking failed: {e}")
            return self._fallback_response(question)

    def _add_to_memory(self, role: str, content: str, metadata: Optional[Dict] = None):
        """Add message to problem-focused memory."""
        message = Message(
            role=role,
            content=content,
            timestamp=__import__('datetime').datetime.now(),
            metadata=metadata or {'type': 'diagnosis'},
        )
        self.memory.append(message)

    def _execute_function_calls(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Parse and execute diagnostic tool calls.

        Focus on: detect_anomalies, analyze_data_quality, find_root_causes, assess_impact
        """
        if not self.tool_registry:
            return None

        try:
            function_calls = FunctionCallParser.extract_function_calls(response)

            if not function_calls:
                logger.debug("No function calls found")
                return None

            logger.info(f"Found {len(function_calls)} diagnostic tool call(s)")
            results = {}

            for call in function_calls:
                try:
                    logger.debug(f"Executing diagnostic tool: {call.function}")

                    # Check if it's a diagnostic tool
                    diagnostic_tools = [
                        'detect_anomalies',
                        'analyze_data_quality',
                        'find_root_causes',
                        'assess_impact',
                        'detect_outliers',
                        'check_data_quality'
                    ]

                    if call.function not in diagnostic_tools:
                        logger.warning(f"Skipping non-diagnostic tool: {call.function}")
                        continue

                    result = self.tool_registry.call(call.function, **call.parameters)
                    results[call.function] = result

                    logger.info(f"Diagnostic tool executed: {call.function}")

                except Exception as e:
                    logger.error(f"Diagnostic tool execution failed ({call.function}): {e}")
                    results[call.function] = {'error': str(e)}

            return results if results else None

        except Exception as e:
            logger.error(f"Error executing diagnostic tools: {e}")
            return None

    def _fallback_response(self, question: str) -> str:
        """Fallback response when LLM unavailable."""
        logger.warning("LLM provider unavailable for diagnosis")
        return (
            "Diagnostic analysis unavailable. "
            f"Problem: {question[:100]}... "
            "Please ensure LLM provider is available."
        )

    def health_check(self) -> Dict[str, Any]:
        """Check agent health with issue tracking info."""
        base_health = super().health_check()
        base_health.update({
            'memory_type': 'problem-focused',
            'memory_size': len(self.memory) if self.memory else 0,
            'issues_tracked': len(self.issues),
        })
        return base_health
