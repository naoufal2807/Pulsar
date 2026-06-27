# tests/intelligence/test_schema_agent.py
"""Unit tests for SchemaAgent."""

import pytest
import polars as pl

from pulsar.core.intelligence.schema_agent import SchemaAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry


SCHEMA_TOOLS = {
    "describe_dataset",
    "infer_domain",
    "identify_key_entities",
    "extract_key_metrics",
    "describe_patterns",
}


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "show_id": ["s1", "s2", "s3"],
        "title": ["Stranger Things", "Narcos", "Dark"],
        "type": ["TV Show", "TV Show", "TV Show"],
        "release_year": [2016, 2015, 2017],
        "rating": ["TV-14", "TV-MA", "TV-MA"],
    })


@pytest.fixture
def agent(sample_df):
    return SchemaAgent(df=sample_df)


class TestSchemaAgentInit:
    def test_stage_name(self, agent):
        assert agent.STAGE_NAME == "schema"
        assert agent.stage_name == "schema"

    def test_required_keys_empty(self):
        assert SchemaAgent.REQUIRED_KEYS == []

    def test_tools_enabled_with_df(self, agent):
        assert agent.tools_enabled is True
        assert agent.tool_registry is not None

    def test_tools_disabled_without_df(self):
        agent = SchemaAgent(df=None)
        assert agent.tool_registry is None


class TestSchemaAgentRegistry:
    def test_exactly_five_tools(self, agent):
        assert len(agent.tool_registry.tools) == 5

    def test_owns_correct_tools(self, agent):
        assert set(agent.tool_registry.tools.keys()) == SCHEMA_TOOLS

    def test_no_quality_tools(self, agent):
        assert "check_data_quality" not in agent.tool_registry.tools
        assert "detect_outliers" not in agent.tool_registry.tools

    def test_no_stats_tools(self, agent):
        assert "compute_statistics" not in agent.tool_registry.tools
        assert "analyze_correlation" not in agent.tool_registry.tools

    def test_create_registry_classmethod(self, sample_df):
        registry = SchemaAgent._create_registry(sample_df)
        assert set(registry.tools.keys()) == SCHEMA_TOOLS

    def test_tool_schemas_valid(self, agent):
        schemas = agent.tool_registry.get_schemas()
        assert len(schemas) == 5
        for schema in schemas:
            assert "name" in schema
            assert "description" in schema
            assert schema["name"] in SCHEMA_TOOLS


class TestSchemaAgentPrompt:
    def test_system_prompt_loaded(self, agent):
        assert agent.system_prompt is not None
        assert len(agent.system_prompt) > 0

    def test_system_prompt_mentions_schema(self, agent):
        assert "schema" in agent.system_prompt.lower() or "domain" in agent.system_prompt.lower()


class TestSchemaAgentHealthCheck:
    def test_health_check_tool_count(self, agent):
        health = agent.health_check()
        assert health["tools_available"] == 5

    def test_health_check_agent_type(self, agent):
        health = agent.health_check()
        assert health["agent_type"] == "SchemaAgent"


class TestAgentRegistrySchemaKey:
    def test_registry_has_schema_key(self):
        assert "schema" in AgentRegistry.list_agents()

    def test_registry_creates_schema_agent(self, sample_df):
        agent = AgentRegistry.get_agent("schema", df=sample_df)
        assert isinstance(agent, SchemaAgent)

    def test_schema_agent_tool_count_via_registry(self, sample_df):
        agent = AgentRegistry.get_agent("schema", df=sample_df)
        assert len(agent.tool_registry.tools) == 5
