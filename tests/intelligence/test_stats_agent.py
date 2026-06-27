# tests/intelligence/test_stats_agent.py
"""Unit tests for StatsAgent."""

import pytest
import polars as pl

from pulsar.core.intelligence.stats_agent import StatsAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry


STATS_TOOLS = {
    "compute_statistics",
    "analyze_correlation",
    "get_top_values",
    "analyze_concentration",
    "analyze_distribution_skewness",
    "analyze_variability",
    "analyze_relationships",
    "find_top_performers",
}


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "show_id": ["s1", "s2", "s3"],
        "title": ["Stranger Things", "Narcos", "Dark"],
        "type": ["TV Show", "TV Show", "Movie"],
        "release_year": [2016, 2015, 2017],
        "duration_min": [50, 45, 60],
    })


@pytest.fixture
def agent(sample_df):
    return StatsAgent(df=sample_df)


class TestStatsAgentInit:
    def test_stage_name(self, agent):
        assert agent.STAGE_NAME == "stats"
        assert agent.stage_name == "stats"

    def test_required_keys_empty(self):
        assert StatsAgent.REQUIRED_KEYS == []

    def test_tools_enabled_with_df(self, agent):
        assert agent.tools_enabled is True
        assert agent.tool_registry is not None

    def test_tools_disabled_without_df(self):
        agent = StatsAgent(df=None)
        assert agent.tool_registry is None


class TestStatsAgentRegistry:
    def test_exactly_eight_tools(self, agent):
        assert len(agent.tool_registry.tools) == 8

    def test_owns_correct_tools(self, agent):
        assert set(agent.tool_registry.tools.keys()) == STATS_TOOLS

    def test_no_schema_tools(self, agent):
        assert "describe_dataset" not in agent.tool_registry.tools
        assert "infer_domain" not in agent.tool_registry.tools

    def test_no_quality_tools(self, agent):
        assert "check_data_quality" not in agent.tool_registry.tools
        assert "detect_outliers" not in agent.tool_registry.tools

    def test_create_registry_classmethod(self, sample_df):
        registry = StatsAgent._create_registry(sample_df)
        assert set(registry.tools.keys()) == STATS_TOOLS

    def test_tool_schemas_valid(self, agent):
        schemas = agent.tool_registry.get_schemas()
        assert len(schemas) == 8
        for schema in schemas:
            assert "name" in schema
            assert "description" in schema
            assert schema["name"] in STATS_TOOLS


class TestStatsAgentPrompt:
    def test_system_prompt_loaded(self, agent):
        assert agent.system_prompt is not None
        assert len(agent.system_prompt) > 0

    def test_system_prompt_mentions_stats(self, agent):
        prompt_lower = agent.system_prompt.lower()
        assert "stat" in prompt_lower or "distribution" in prompt_lower or "correlation" in prompt_lower


class TestStatsAgentHealthCheck:
    def test_health_check_tool_count(self, agent):
        health = agent.health_check()
        assert health["tools_available"] == 8

    def test_health_check_agent_type(self, agent):
        health = agent.health_check()
        assert health["agent_type"] == "StatsAgent"


class TestAgentRegistryStatsKey:
    def test_registry_has_stats_key(self):
        assert "stats" in AgentRegistry.list_agents()

    def test_registry_creates_stats_agent(self, sample_df):
        agent = AgentRegistry.get_agent("stats", df=sample_df)
        assert isinstance(agent, StatsAgent)

    def test_stats_agent_tool_count_via_registry(self, sample_df):
        agent = AgentRegistry.get_agent("stats", df=sample_df)
        assert len(agent.tool_registry.tools) == 8
