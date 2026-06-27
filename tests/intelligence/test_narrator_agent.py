# tests/intelligence/test_narrator_agent.py
"""Unit tests for NarratorAgent."""

import pytest
import polars as pl

from pulsar.core.intelligence.narrator_agent import NarratorAgent
from pulsar.core.intelligence.agent_registry import AgentRegistry
from pulsar.core.intelligence.shared_state_store import SharedStateStore


@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "show_id": ["s1", "s2", "s3"],
        "title": ["Stranger Things", "Narcos", "Dark"],
        "type": ["TV Show", "TV Show", "Movie"],
        "release_year": [2016, 2015, 2017],
    })


@pytest.fixture
def agent(sample_df):
    return NarratorAgent(df=sample_df)


class TestNarratorAgentInit:
    def test_stage_name(self, agent):
        assert agent.STAGE_NAME == "narrator"
        assert agent.stage_name == "narrator"

    def test_required_keys_declared(self):
        assert "schema.findings" in NarratorAgent.REQUIRED_KEYS
        assert "quality.findings" in NarratorAgent.REQUIRED_KEYS
        assert "stats.findings" in NarratorAgent.REQUIRED_KEYS

    def test_tools_always_disabled(self, agent):
        assert agent.tools_enabled is False

    def test_no_tool_registry(self, agent):
        assert agent.tool_registry is None

    def test_tools_disabled_without_df(self):
        narrator = NarratorAgent(df=None)
        assert narrator.tools_enabled is False
        assert narrator.tool_registry is None


class TestNarratorAgentPrompt:
    def test_system_prompt_loaded(self, agent):
        assert agent.system_prompt is not None
        assert len(agent.system_prompt) > 0

    def test_system_prompt_mentions_synthesis(self, agent):
        prompt_lower = agent.system_prompt.lower()
        assert (
            "synthesis" in prompt_lower
            or "synthesize" in prompt_lower
            or "verdict" in prompt_lower
            or "schema" in prompt_lower
        )


class TestNarratorAgentHealthCheck:
    def test_health_check_tool_count_zero(self, agent):
        health = agent.health_check()
        assert health["tools_available"] == 0

    def test_health_check_agent_type(self, agent):
        health = agent.health_check()
        assert health["agent_type"] == "NarratorAgent"

    def test_health_check_tools_disabled(self, agent):
        health = agent.health_check()
        assert health["tools_enabled"] is False


class TestNarratorAgentSharedState:
    def test_reads_required_keys_from_shared_state_registry(self):
        keys = SharedStateStore.get_required_keys("narrator")
        assert "schema.findings" in keys
        assert "quality.findings" in keys
        assert "stats.findings" in keys

    def test_narrator_stage_not_in_tier1_dependency(self):
        for stage in ("schema", "quality", "stats"):
            keys = SharedStateStore.get_required_keys(stage)
            assert keys == [], f"{stage} should have no dependencies"

    def test_shared_state_injected(self, sample_df):
        state = SharedStateStore()
        narrator = NarratorAgent(df=sample_df, shared_state=state)
        assert narrator.shared_state is state


class TestAgentRegistryNarratorKey:
    def test_registry_has_narrator_key(self):
        assert "narrator" in AgentRegistry.list_agents()

    def test_registry_creates_narrator_agent(self, sample_df):
        agent = AgentRegistry.get_agent("narrator", df=sample_df)
        assert isinstance(agent, NarratorAgent)

    def test_narrator_via_registry_has_no_tools(self, sample_df):
        agent = AgentRegistry.get_agent("narrator", df=sample_df)
        assert agent.tools_enabled is False
        assert agent.tool_registry is None
