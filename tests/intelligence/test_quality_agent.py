# tests/intelligence/test_quality_agent.py
"""Unit tests for QualityAgent."""

import pytest
import polars as pl

from pulsar.core.intelligence.quality_agent import QualityAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry


QUALITY_TOOLS = {
    "check_data_quality",
    "detect_outliers",
    "explain_outliers",
}


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "show_id": ["s1", "s2", "s3", "s4"],
        "title": ["Stranger Things", "Narcos", "Dark", None],
        "type": ["TV Show", "TV Show", "TV Show", "Movie"],
        "release_year": [2016, 2015, 2017, 2020],
        "rating": ["TV-14", "TV-MA", "TV-MA", "TV-G"],
    })


@pytest.fixture
def agent(sample_df):
    return QualityAgent(df=sample_df)


class TestQualityAgentInit:
    def test_stage_name(self, agent):
        assert agent.STAGE_NAME == "quality"
        assert agent.stage_name == "quality"

    def test_required_keys_empty(self):
        assert QualityAgent.REQUIRED_KEYS == []

    def test_tools_enabled_with_df(self, agent):
        assert agent.tools_enabled is True
        assert agent.tool_registry is not None

    def test_tools_disabled_without_df(self):
        agent = QualityAgent(df=None)
        assert agent.tool_registry is None


class TestQualityAgentRegistry:
    def test_exactly_three_tools(self, agent):
        assert len(agent.tool_registry.tools) == 3

    def test_owns_correct_tools(self, agent):
        assert set(agent.tool_registry.tools.keys()) == QUALITY_TOOLS

    def test_no_schema_tools(self, agent):
        assert "describe_dataset" not in agent.tool_registry.tools
        assert "infer_domain" not in agent.tool_registry.tools

    def test_no_stats_tools(self, agent):
        assert "compute_statistics" not in agent.tool_registry.tools
        assert "analyze_correlation" not in agent.tool_registry.tools

    def test_create_registry_classmethod(self, sample_df):
        registry = QualityAgent._create_registry(sample_df)
        assert set(registry.tools.keys()) == QUALITY_TOOLS

    def test_tool_schemas_valid(self, agent):
        schemas = agent.tool_registry.get_schemas()
        assert len(schemas) == 3
        for schema in schemas:
            assert "name" in schema
            assert "description" in schema
            assert schema["name"] in QUALITY_TOOLS


class TestQualityAgentPrompt:
    def test_system_prompt_loaded(self, agent):
        assert agent.system_prompt is not None
        assert len(agent.system_prompt) > 0

    def test_system_prompt_mentions_quality(self, agent):
        prompt_lower = agent.system_prompt.lower()
        assert "quality" in prompt_lower or "outlier" in prompt_lower


class TestQualityAgentHealthCheck:
    def test_health_check_tool_count(self, agent):
        health = agent.health_check()
        assert health["tools_available"] == 3

    def test_health_check_agent_type(self, agent):
        health = agent.health_check()
        assert health["agent_type"] == "QualityAgent"


class TestAgentRegistryQualityKey:
    def test_registry_has_quality_key(self):
        assert "quality" in AgentRegistry.list_agents()

    def test_registry_creates_quality_agent(self, sample_df):
        agent = AgentRegistry.get_agent("quality", df=sample_df)
        assert isinstance(agent, QualityAgent)

    def test_quality_agent_tool_count_via_registry(self, sample_df):
        agent = AgentRegistry.get_agent("quality", df=sample_df)
        assert len(agent.tool_registry.tools) == 3
