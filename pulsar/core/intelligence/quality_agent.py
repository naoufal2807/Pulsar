# pulsar/core/intelligence/quality_agent.py
"""QualityAgent: data reliability (nulls, outliers, integrity checks)."""

import logging
from typing import Optional

import polars as pl

from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.tools import ToolRegistry, ToolDefinition, ToolParameter
from pulsar.core.intelligence.quality_tools import (
    check_data_quality,
    detect_outliers,
    explain_outliers,
)
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)


class QualityAgent(ReasoningAgent):
    """
    Domain-specialized agent for data quality analysis.

    Owns exclusively: check_data_quality, detect_outliers,
    explain_outliers (3 tools).

    Reads: nothing (runs concurrently with SchemaAgent and StatsAgent).
    Writes to SharedStateStore: "quality" key.
    """

    STAGE_NAME = "quality"
    REQUIRED_KEYS = []  # runs concurrently with other first-tier agents

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
            system_prompt = PromptSystem().render("domain_quality")
        except Exception as e:
            logger.warning(f"Prompt load failed for domain_quality: {e}")
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

    def think(self, question: str) -> str:
        """Run quality analysis then write typed keys to SharedStateStore."""
        response = super().think(question)
        self._write_typed_keys()
        return response

    def _write_typed_keys(self) -> None:
        """Write deterministic quality keys after tool execution."""
        if not self.shared_state or self.df is None:
            return
        null_report: dict = {}
        for col in self.df.columns:
            result = check_data_quality(self.df, col)
            if "error" not in result:
                null_report[col] = result["null_percent"]

        max_null_pct = max(null_report.values()) if null_report else 0.0
        if max_null_pct > 50:
            verdict = "BLOCK"
        elif max_null_pct > 10:
            verdict = "WARN"
        else:
            verdict = "CLEAN"

        issues = [
            f"{col}: {pct:.1f}% nulls"
            for col, pct in null_report.items()
            if pct > 10
        ]

        self.shared_state.set(
            "quality.verdict", verdict, stage="quality"
        )
        self.shared_state.set(
            "quality.null_report", null_report, stage="quality"
        )
        if issues:
            self.shared_state.set(
                "quality.issues", issues, stage="quality"
            )
        logger.info(
            f"[QualityAgent] verdict={verdict}, "
            f"max_null={max_null_pct:.1f}%, issues={len(issues)}"
        )

    @classmethod
    def _create_registry(cls, df: pl.DataFrame) -> ToolRegistry:
        """Quality-only tool registry — 3 tools."""
        registry = ToolRegistry()

        registry.register(ToolDefinition(
            name="check_data_quality",
            description=(
                "Check data quality metrics for a column "
                "(null counts, distinct values, duplicates)"
            ),
            parameters=[
                ToolParameter("column", "string", "Column name", required=True),
            ],
            function=lambda column: check_data_quality(df, column),
        ))
        registry.register(ToolDefinition(
            name="detect_outliers",
            description=(
                "Detect outliers in a numeric column using IQR method "
                "(Q1-1.5×IQR, Q3+1.5×IQR bounds)"
            ),
            parameters=[
                ToolParameter("column", "string", "Column name", required=True),
            ],
            function=lambda column: detect_outliers(df, column),
        ))
        registry.register(ToolDefinition(
            name="explain_outliers",
            description=(
                "Explain business significance of outliers "
                "across all columns"
            ),
            parameters=[],
            function=lambda: explain_outliers(df),
        ))

        return registry
