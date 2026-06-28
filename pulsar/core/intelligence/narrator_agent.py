# pulsar/core/intelligence/narrator_agent.py
"""NarratorAgent: synthesizes schema + quality + stats into a human verdict."""

import logging
from typing import Optional

import polars as pl

from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)

_SYNTHESIS_QUESTION = (
    "You have access to the full analysis from SchemaAgent, QualityAgent, "
    "and StatsAgent in your context. Synthesize their findings into a concise "
    "5-sentence verdict using the output format from your system prompt:\n"
    "  DATASET: ...\n"
    "  DOMAIN: ...\n"
    "  QUALITY: ...\n"
    "  KEY INSIGHT: ...\n"
    "  RECOMMENDATION: ..."
)


class NarratorAgent(ReasoningAgent):
    """
    Domain-specialized agent for narrative synthesis.

    Reads from SharedStateStore:
      schema.findings, schema.response_summary
      quality.findings, quality.response_summary
      stats.findings,  stats.response_summary

    Calls NO tools (tools_enabled=False permanently).
    Writes to SharedStateStore: "narrator" stage keys.
    """

    STAGE_NAME = "narrator"
    REQUIRED_KEYS = [
        "schema.findings",
        "schema.response_summary",
        "quality.findings",
        "quality.response_summary",
        "stats.findings",
        "stats.response_summary",
    ]

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
        tools_enabled: Optional[bool] = None,  # registry compat; always ignored
        **kwargs,
    ):
        from pulsar.core.intelligence.prompt_system import PromptSystem
        try:
            system_prompt = PromptSystem().render("domain_narrator")
        except Exception as e:
            logger.warning(f"Prompt load failed for domain_narrator: {e}")
            system_prompt = None

        super().__init__(
            llm_config=llm_config,
            df=df,
            tools_enabled=False,
            system_prompt=system_prompt,
            stage_name=self.STAGE_NAME,
            conversation_id=conversation_id,
            dataset_name=dataset_name,
            shared_state=shared_state,
        )
        # NarratorAgent never has a tool registry
        self.tool_registry = None
        self.tools_enabled = False

    def synthesize(self) -> str:
        """
        Read schema/quality/stats findings from shared state and produce
        a 5-sentence human-readable verdict.

        Returns:
            Narrative verdict string.
        """
        return self.think(_SYNTHESIS_QUESTION)
