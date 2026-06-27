# pulsar/core/intelligence/stats_agent.py
"""StatsAgent: numerical analysis (distributions, correlations, variability)."""

import logging
from typing import Optional

import polars as pl

from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.tools import ToolRegistry, ToolDefinition, ToolParameter
from pulsar.core.intelligence.stats_tools import (
    compute_statistics,
    analyze_correlation,
    get_top_values,
    analyze_concentration,
    analyze_distribution_skewness,
    analyze_variability,
    analyze_relationships,
    find_top_performers,
)
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)


class StatsAgent(ReasoningAgent):
    """
    Domain-specialized agent for numerical analysis.

    Owns exclusively: compute_statistics, analyze_correlation,
    get_top_values, analyze_concentration,
    analyze_distribution_skewness, analyze_variability,
    analyze_relationships, find_top_performers (8 tools).

    Reads: nothing (runs concurrently with SchemaAgent).
    Writes to SharedStateStore: "stats" key.
    """

    STAGE_NAME = "stats"
    REQUIRED_KEYS = []  # runs concurrently with SchemaAgent

    def __init__(
        self,
        llm_config: Optional[LLMConfig] = None,
        df: Optional[pl.DataFrame] = None,
        conversation_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        shared_state: Optional[SharedStateStore] = None,
        tools_enabled: Optional[bool] = None,  # registry compat; ignored
        **kwargs,
    ):
        from pulsar.core.intelligence.prompt_system import PromptSystem
        try:
            system_prompt = PromptSystem().render("domain_stats")
        except Exception:
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

        if df is not None:
            self.tool_registry = self._create_registry(df)
            self.tools_enabled = True

    @classmethod
    def _create_registry(cls, df: pl.DataFrame) -> ToolRegistry:
        """Stats-only tool registry — 8 tools."""
        registry = ToolRegistry()

        registry.register(ToolDefinition(
            name="compute_statistics",
            description=(
                "Compute statistical metrics (mean, median, std, "
                "min, max) for a numeric column"
            ),
            parameters=[
                ToolParameter("column", "string", "Column name", required=True),
            ],
            function=lambda column: compute_statistics(df, column),
        ))
        registry.register(ToolDefinition(
            name="analyze_correlation",
            description="Analyze Pearson correlation between two numeric columns",
            parameters=[
                ToolParameter("col1", "string", "First column", required=True),
                ToolParameter("col2", "string", "Second column", required=True),
            ],
            function=lambda col1, col2: analyze_correlation(df, col1, col2),
        ))
        registry.register(ToolDefinition(
            name="get_top_values",
            description="Get top N values for a column with their counts",
            parameters=[
                ToolParameter("column", "string", "Column name", required=True),
                ToolParameter(
                    "limit", "number",
                    "Number of top values to return",
                    required=False,
                ),
            ],
            function=lambda column, limit=10: get_top_values(df, column, limit),
        ))
        registry.register(ToolDefinition(
            name="analyze_concentration",
            description=(
                "Analyze market concentration using Herfindahl index"
            ),
            parameters=[],
            function=lambda: analyze_concentration(df),
        ))
        registry.register(ToolDefinition(
            name="analyze_distribution_skewness",
            description=(
                "Analyze distribution skewness "
                "(right-skewed, left-skewed, normal)"
            ),
            parameters=[],
            function=lambda: analyze_distribution_skewness(df),
        ))
        registry.register(ToolDefinition(
            name="analyze_variability",
            description="Analyze data consistency and variability across columns",
            parameters=[],
            function=lambda: analyze_variability(df),
        ))
        registry.register(ToolDefinition(
            name="analyze_relationships",
            description=(
                "Analyze relationships and correlations between columns"
            ),
            parameters=[],
            function=lambda: analyze_relationships(df),
        ))
        registry.register(ToolDefinition(
            name="find_top_performers",
            description="Find top performing entities by various metrics",
            parameters=[],
            function=lambda: find_top_performers(df),
        ))

        return registry
