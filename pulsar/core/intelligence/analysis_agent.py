"""AnalysisAgent: Lightweight, single-shot analysis without iteration."""

from typing import Any, Dict, List, Optional
import logging

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
    """

    def _init_memory(self) -> None:
        """Initialize stateless memory (None)."""
        self.memory = None
        logger.debug(f"{self.__class__.__name__} initialized with stateless memory")

    def think(self, question: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Think through a question with single-shot analysis.

        Args:
            question: The question to analyze
            context: Optional context dictionary

        Returns:
            Agent's response/analysis (no tools called)
        """
        if not self.provider_available:
            return self._fallback_response(question)

        try:
            logger.debug(f"AnalysisAgent.think() called: {question[:80]}...")

            # Build prompt (no memory context since stateless)
            prompt_parts = [self.system_prompt]

            # Add context if provided
            if context:
                prompt_parts.append("\nContext:")
                for key, value in context.items():
                    prompt_parts.append(f"- {key}: {value}")

            # Add current question
            prompt_parts.append(f"\nQuestion: {question}")

            prompt = "\n".join(prompt_parts)

            # Single LLM call (no tool iteration)
            logger.debug("Calling LLM for single-shot analysis...")
            response = self.llm_provider.generate(prompt)

            logger.debug(f"Response received: {len(response)} chars")
            return response

        except Exception as e:
            logger.error(f"AnalysisAgent thinking failed: {e}")
            return self._fallback_response(question)

    def _fallback_response(self, question: str) -> str:
        """Fallback response when LLM unavailable."""
        logger.warning("LLM provider unavailable, using fallback")
        return (
            "Analysis provider is currently unavailable. "
            f"Please try again later. Question was: {question[:100]}..."
        )

    def health_check(self) -> Dict[str, Any]:
        """Check agent health with stateless memory info."""
        base_health = super().health_check()
        base_health.update({
            'memory_type': 'stateless',
            'memory_size': None,
        })
        return base_health
