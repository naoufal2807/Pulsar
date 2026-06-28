# pulsar/core/intelligence/schema_agent.py
"""SchemaAgent: structural identity of a dataset."""

import logging
from typing import Optional

import polars as pl

from pulsar.core.intelligence.agent import ReasoningAgent
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.tools import (
    ToolRegistry,
    ToolDefinition,
)
from pulsar.core.intelligence.schema_tools import (
    describe_dataset,
    infer_domain,
    identify_key_entities,
    extract_key_metrics,
    describe_patterns,
)
from pulsar.core.llm_connectors import LLMConfig

logger = logging.getLogger(__name__)


class SchemaAgent(ReasoningAgent):
    """
    Domain-specialized agent for structural dataset understanding.

    Owns exclusively: describe_dataset, infer_domain,
    identify_key_entities, extract_key_metrics, describe_patterns
    (5 tools).

    Reads: nothing (first agent, no upstream dependencies).
    Writes to SharedStateStore: "schema" key.
    """

    STAGE_NAME = "schema"
    REQUIRED_KEYS = []  # first agent — no upstream dependencies

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
            system_prompt = PromptSystem().render("domain_schema")
        except Exception as e:
            logger.warning(f"Prompt load failed for domain_schema: {e}")
            system_prompt = None

        # Pass tools_enabled=False so base does not create the
        # 16-tool registry. We replace it below with 5 schema tools.
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
        """Run schema analysis then write typed keys to SharedStateStore."""
        response = super().think(question)
        self._write_typed_keys()
        return response

    def _write_typed_keys(self) -> None:
        """Write deterministic schema keys after tool execution."""
        if not self.shared_state or self.df is None:
            return
        df = self.df
        self.shared_state.set(
            "schema.columns",
            [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns],
            stage="schema",
        )
        self.shared_state.set(
            "schema.types",
            {c: str(df[c].dtype) for c in df.columns},
            stage="schema",
        )
        self.shared_state.set(
            "schema.cardinality",
            {c: df[c].n_unique() for c in df.columns},
            stage="schema",
        )
        logger.info(
            f"[SchemaAgent] wrote typed keys for {len(df.columns)} columns"
        )

    @classmethod
    def _create_registry(cls, df: pl.DataFrame) -> ToolRegistry:
        """Schema-only tool registry — 5 tools."""
        registry = ToolRegistry()

        registry.register(ToolDefinition(
            name="describe_dataset",
            description=(
                "Get overall dataset information "
                "(rows, columns, data types, memory)"
            ),
            parameters=[],
            function=lambda: describe_dataset(df),
        ))
        registry.register(ToolDefinition(
            name="infer_domain",
            description=(
                "Determine what business domain or industry "
                "this dataset represents"
            ),
            parameters=[],
            function=lambda: infer_domain(df),
        ))
        registry.register(ToolDefinition(
            name="identify_key_entities",
            description=(
                "Identify main entities and key terms "
                "in the dataset"
            ),
            parameters=[],
            function=lambda: identify_key_entities(df),
        ))
        registry.register(ToolDefinition(
            name="extract_key_metrics",
            description=(
                "Extract important numeric metrics "
                "from the dataset"
            ),
            parameters=[],
            function=lambda: extract_key_metrics(df),
        ))
        registry.register(ToolDefinition(
            name="describe_patterns",
            description=(
                "Describe discovered structural patterns "
                "in the dataset"
            ),
            parameters=[],
            function=lambda: describe_patterns(df),
        ))

        return registry
