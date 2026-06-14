"""AnalysisAgent: Lightweight, single-shot analysis without iteration."""

from typing import Any, Dict, List, Optional
import logging
import traceback
from datetime import datetime

import polars as pl

from pulsar.core.intelligence.agent_base import Agent
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)


class AnalysisAgent(Agent):
    """
    Lightweight analysis agent for quick insights.

    Memory strategy: Stateless (no conversation history)
    Tool behavior: Single-shot (no iteration)
    Typical use: Quick analysis, fast responses, no context needed

    Ideal for:
    - Quick dataset overviews
    - Single metric calculations
    - Fast insights without iteration

    Error Handling:
    - Validates input parameters
    - Logs all errors with tracebacks
    - No silent failures
    - Explicit fallback messages
    """

    def _init_memory(self) -> None:
        """Initialize stateless memory (None)."""
        self.memory = None
        self.execution_count = 0
        self.error_count = 0
        logger.debug(
            f"{self.__class__.__name__} initialized with stateless memory",
            extra={'agent_type': self.__class__.__name__, 'memory_type': 'stateless'}
        )

    def think(self, question: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Think through a question with single-shot analysis.

        Args:
            question: The question to analyze (must be non-empty string)
            context: Optional context dictionary

        Returns:
            Agent's response/analysis (no tools called)

        Raises:
            No exceptions raised - returns fallback on error
        """
        # Input validation
        if not question or not isinstance(question, str):
            error_msg = f"Invalid question: must be non-empty string, got {type(question)}"
            logger.error(error_msg, extra={'input_type': type(question).__name__})
            return f"ERROR: {error_msg}"

        if context is not None and not isinstance(context, dict):
            error_msg = f"Invalid context: must be dict, got {type(context)}"
            logger.error(error_msg, extra={'context_type': type(context).__name__})
            return f"ERROR: {error_msg}"

        self.execution_count += 1
        start_time = datetime.now()

        logger.info(
            f"AnalysisAgent.think() started (execution #{self.execution_count})",
            extra={
                'agent_type': self.__class__.__name__,
                'question_length': len(question),
                'has_context': context is not None,
            }
        )

        # Provider availability check
        if not self.provider_available:
            logger.warning(
                "LLM provider unavailable at think() time",
                extra={'provider_status': 'unavailable'}
            )
            return self._fallback_response(question)

        try:
            # Build prompt (no memory context since stateless)
            prompt_parts = [self.system_prompt]

            # Add context if provided (with validation)
            if context:
                try:
                    prompt_parts.append("\nContext:")
                    for key, value in context.items():
                        if not isinstance(key, str):
                            logger.warning(
                                f"Non-string context key: {type(key).__name__}",
                                extra={'key': str(key)[:100]}
                            )
                            continue
                        prompt_parts.append(f"- {key}: {str(value)[:500]}")
                except Exception as e:
                    logger.error(
                        f"Error processing context: {e}",
                        extra={'error': str(e), 'traceback': traceback.format_exc()}
                    )
                    # Continue without context rather than failing entirely

            # Add current question
            prompt_parts.append(f"\nQuestion: {question}")

            prompt = "\n".join(prompt_parts)
            logger.debug(
                "Prompt constructed",
                extra={'prompt_length': len(prompt)}
            )

            # Single LLM call (no tool iteration)
            logger.debug("Calling LLM for single-shot analysis...")
            response = self.llm_provider.generate(prompt)

            # Validate response
            if not response:
                logger.error(
                    "LLM returned empty response",
                    extra={'response_length': 0}
                )
                return self._fallback_response(question)

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"AnalysisAgent.think() completed successfully (execution #{self.execution_count})",
                extra={
                    'response_length': len(response),
                    'duration_seconds': duration,
                    'execution_count': self.execution_count,
                }
            )
            return response

        except Exception as e:
            self.error_count += 1
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"AnalysisAgent.think() failed: {type(e).__name__}: {e}",
                extra={
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'duration_seconds': duration,
                    'execution_count': self.execution_count,
                    'error_count': self.error_count,
                    'traceback': traceback.format_exc(),
                }
            )
            return self._fallback_response(question)

    def _fallback_response(self, question: str) -> str:
        """
        Fallback response when LLM unavailable.

        Logs the fallback invocation for debugging.
        """
        logger.warning(
            "Returning fallback response (LLM provider unavailable or error)",
            extra={
                'agent_type': self.__class__.__name__,
                'fallback_reason': 'provider_unavailable_or_error',
                'question_truncated': question[:100],
            }
        )
        return (
            "Analysis provider is currently unavailable. "
            f"Please try again later. Question was: {question[:100]}..."
        )

    def health_check(self) -> Dict[str, Any]:
        """Check agent health with stateless memory info and metrics."""
        try:
            base_health = super().health_check()
            base_health.update({
                'memory_type': 'stateless',
                'memory_size': None,
                'execution_count': self.execution_count,
                'error_count': self.error_count,
                'error_rate': (
                    self.error_count / self.execution_count
                    if self.execution_count > 0 else 0
                ),
            })
            logger.debug(
                "Health check completed",
                extra={'health': base_health}
            )
            return base_health
        except Exception as e:
            logger.error(
                f"Health check failed: {e}",
                extra={'error': str(e), 'traceback': traceback.format_exc()}
            )
            return {
                'agent_type': self.__class__.__name__,
                'health_check_status': 'FAILED',
                'error': str(e),
            }
