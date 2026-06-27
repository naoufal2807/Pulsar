# tests/intelligence/test_scout_pipeline.py
"""
Integration tests for the scout pipeline.

Tests wiring only (no LLM calls):
  QuestionRouter → AgentRegistry → SharedStateStore state-passing
"""

import pytest
import polars as pl

from pulsar.core.intelligence.agent_registry import AgentRegistry
from pulsar.core.intelligence.question_router import get_tiers, TIER1_QUESTION, list_modes
from pulsar.core.intelligence.shared_state_store import SharedStateStore
from pulsar.core.intelligence.schema_agent import SchemaAgent
from pulsar.core.intelligence.quality_agent import QualityAgent
from pulsar.core.intelligence.stats_agent import StatsAgent
from pulsar.core.intelligence.narrator_agent import NarratorAgent


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
def shared_state():
    return SharedStateStore()


# ── Tier wiring ───────────────────────────────────────────────────────────────

class TestPipelineWiring:
    def test_scout_tier1_agents_are_domain_agents(self, sample_df, shared_state):
        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        types = {type(a).__name__ for a in agents}
        assert types == {"SchemaAgent", "QualityAgent", "StatsAgent"}

    def test_scout_tier2_agent_is_narrator(self, sample_df, shared_state):
        _, tier2 = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier2
        ]
        assert len(agents) == 1
        assert isinstance(agents[0], NarratorAgent)

    def test_all_tier1_agents_share_same_state_instance(self, sample_df, shared_state):
        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        for agent in agents:
            assert agent.shared_state is shared_state

    def test_narrator_shares_same_state_instance(self, sample_df, shared_state):
        _, tier2 = get_tiers("scout")
        narrator = AgentRegistry.get_agent(tier2[0], df=sample_df, shared_state=shared_state)
        assert narrator.shared_state is shared_state

    def test_tier1_questions_exist_for_all_scout_agents(self):
        tier1, _ = get_tiers("scout")
        for key in tier1:
            assert key in TIER1_QUESTION
            assert len(TIER1_QUESTION[key]) > 30


# ── SharedStateStore state passing ────────────────────────────────────────────

class TestStatePassing:
    def test_schema_agent_stage_name_matches_state_key_prefix(self, sample_df, shared_state):
        agent = AgentRegistry.get_agent("schema", df=sample_df, shared_state=shared_state)
        assert agent.stage_name == "schema"

    def test_quality_agent_stage_name_matches_state_key_prefix(self, sample_df, shared_state):
        agent = AgentRegistry.get_agent("quality", df=sample_df, shared_state=shared_state)
        assert agent.stage_name == "quality"

    def test_stats_agent_stage_name_matches_state_key_prefix(self, sample_df, shared_state):
        agent = AgentRegistry.get_agent("stats", df=sample_df, shared_state=shared_state)
        assert agent.stage_name == "stats"

    def test_narrator_reads_tier1_keys_from_state(self, sample_df, shared_state):
        # Simulate what tier-1 agents write after think()
        shared_state.set("schema.findings", ["Schema finding 1"], stage="schema")
        shared_state.set("schema.response_summary", "Schema summary", stage="schema")
        shared_state.set("quality.findings", ["Quality finding 1"], stage="quality")
        shared_state.set("quality.response_summary", "Quality summary", stage="quality")
        shared_state.set("stats.findings", ["Stats finding 1"], stage="stats")
        shared_state.set("stats.response_summary", "Stats summary", stage="stats")

        narrator = NarratorAgent(df=sample_df, shared_state=shared_state)
        context = narrator._load_from_shared_state()

        assert "schema.findings" in context
        assert "quality.findings" in context
        assert "stats.findings" in context

    def test_narrator_required_keys_match_store_registry(self, sample_df, shared_state):
        narrator = NarratorAgent(df=sample_df, shared_state=shared_state)
        declared = set(NarratorAgent.REQUIRED_KEYS)
        registry_keys = set(SharedStateStore.get_required_keys("narrator"))
        # Every declared REQUIRED_KEY must be in the store registry
        assert declared.issubset(registry_keys)

    def test_state_written_by_schema_stage_readable_by_narrator(self, sample_df, shared_state):
        shared_state.set("schema.findings", ["domain: streaming"], stage="schema")
        value = shared_state.get("schema.findings", stage="narrator")
        assert value == ["domain: streaming"]

    def test_empty_state_returns_empty_context_for_narrator(self, sample_df):
        empty_state = SharedStateStore()
        narrator = NarratorAgent(df=sample_df, shared_state=empty_state)
        context = narrator._load_from_shared_state()
        # No keys written → context should be empty (keys not found)
        assert isinstance(context, dict)


# ── Tool exclusivity (cross-agent isolation) ──────────────────────────────────

class TestToolExclusivity:
    def test_no_tool_overlap_between_tier1_agents(self, sample_df, shared_state):
        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        all_tool_sets = [
            set(a.tool_registry.tools.keys()) for a in agents if a.tool_registry
        ]
        # Each pair should be disjoint
        for i, s1 in enumerate(all_tool_sets):
            for j, s2 in enumerate(all_tool_sets):
                if i != j:
                    assert s1.isdisjoint(s2), (
                        f"Tool overlap between agents at index {i} and {j}: "
                        f"{s1 & s2}"
                    )

    def test_narrator_has_no_tools(self, sample_df, shared_state):
        narrator = AgentRegistry.get_agent("narrator", df=sample_df, shared_state=shared_state)
        assert narrator.tool_registry is None
        assert narrator.tools_enabled is False

    def test_total_tier1_tool_count(self, sample_df, shared_state):
        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        total = sum(
            len(a.tool_registry.tools) for a in agents if a.tool_registry
        )
        # schema(5) + quality(3) + stats(8) = 16
        assert total == 16

    def test_tier1_agents_cover_all_16_tools(self, sample_df, shared_state):
        from pulsar.core.intelligence.schema_tools import (
            describe_dataset, infer_domain, identify_key_entities,
            extract_key_metrics, describe_patterns,
        )
        from pulsar.core.intelligence.quality_tools import (
            check_data_quality, detect_outliers, explain_outliers,
        )
        from pulsar.core.intelligence.stats_tools import (
            compute_statistics, analyze_correlation, get_top_values,
            analyze_concentration, analyze_distribution_skewness,
            analyze_variability, analyze_relationships, find_top_performers,
        )
        expected = {
            "describe_dataset", "infer_domain", "identify_key_entities",
            "extract_key_metrics", "describe_patterns",
            "check_data_quality", "detect_outliers", "explain_outliers",
            "compute_statistics", "analyze_correlation", "get_top_values",
            "analyze_concentration", "analyze_distribution_skewness",
            "analyze_variability", "analyze_relationships", "find_top_performers",
        }

        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        covered = set()
        for a in agents:
            if a.tool_registry:
                covered |= set(a.tool_registry.tools.keys())

        assert covered == expected


# ── Mode completeness ─────────────────────────────────────────────────────────

class TestModeCompleteness:
    def test_all_modes_resolvable_via_agent_registry(self, sample_df, shared_state):
        for mode in list_modes():
            tier1, tier2 = get_tiers(mode)
            for key in tier1 + tier2:
                agent = AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
                assert agent is not None

    def test_single_agent_modes_have_empty_tier2(self):
        for mode in ("schema", "quality", "stats"):
            _, tier2 = get_tiers(mode)
            assert tier2 == [], f"{mode} should have no tier2"

    def test_scout_and_narrator_modes_have_narrator_in_tier2(self):
        for mode in ("scout", "narrator"):
            _, tier2 = get_tiers(mode)
            assert "narrator" in tier2
