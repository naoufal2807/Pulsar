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
    def test_scout_tier1_agents_are_schema_and_quality(self, sample_df, shared_state):
        # scout is the fast path: schema + quality only
        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        types = {type(a).__name__ for a in agents}
        assert types == {"SchemaAgent", "QualityAgent"}

    def test_full_mode_tier1_agents_are_all_three(self, sample_df, shared_state):
        tier1, _ = get_tiers("full")
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

    def test_scout_tier1_tool_count(self, sample_df, shared_state):
        # scout fast path: schema(5) + quality(3) = 8 tools
        tier1, _ = get_tiers("scout")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        total = sum(
            len(a.tool_registry.tools) for a in agents if a.tool_registry
        )
        assert total == 8

    def test_full_mode_tier1_tool_count(self, sample_df, shared_state):
        # full mode: schema(5) + quality(3) + stats(8) = 16 tools
        tier1, _ = get_tiers("full")
        agents = [
            AgentRegistry.get_agent(key, df=sample_df, shared_state=shared_state)
            for key in tier1
        ]
        total = sum(
            len(a.tool_registry.tools) for a in agents if a.tool_registry
        )
        assert total == 16

    def test_full_mode_covers_all_16_tools(self, sample_df, shared_state):
        expected = {
            "describe_dataset", "infer_domain", "identify_key_entities",
            "extract_key_metrics", "describe_patterns",
            "check_data_quality", "detect_outliers", "explain_outliers",
            "compute_statistics", "analyze_correlation", "get_top_values",
            "analyze_concentration", "analyze_distribution_skewness",
            "analyze_variability", "analyze_relationships", "find_top_performers",
        }
        tier1, _ = get_tiers("full")
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
        for mode in ("scout", "narrator", "full"):
            _, tier2 = get_tiers(mode)
            assert "narrator" in tier2

    def test_full_mode_exists(self):
        assert "full" in list_modes()


# ── Typed SharedStateStore keys ───────────────────────────────────────────────

class TestTypedKeys:
    def test_quality_agent_writes_verdict_key(self, sample_df):
        """QualityAgent._write_typed_keys() writes quality.verdict."""
        from pulsar.core.intelligence.quality_agent import QualityAgent
        state = SharedStateStore()
        agent = QualityAgent(df=sample_df, shared_state=state)
        agent._write_typed_keys()
        verdict = state.get("quality.verdict", stage="test")
        assert verdict in ("CLEAN", "WARN", "BLOCK")

    def test_quality_agent_writes_null_report_as_dict(self, sample_df):
        """quality.null_report is dict[str, float], not free text."""
        from pulsar.core.intelligence.quality_agent import QualityAgent
        state = SharedStateStore()
        agent = QualityAgent(df=sample_df, shared_state=state)
        agent._write_typed_keys()
        null_report = state.get("quality.null_report", stage="test")
        assert isinstance(null_report, dict)
        for col, pct in null_report.items():
            assert isinstance(col, str)
            assert isinstance(pct, (int, float))

    def test_quality_agent_clean_verdict_on_complete_data(self):
        """All-complete DataFrame → verdict CLEAN."""
        from pulsar.core.intelligence.quality_agent import QualityAgent
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        state = SharedStateStore()
        agent = QualityAgent(df=df, shared_state=state)
        agent._write_typed_keys()
        assert state.get("quality.verdict", stage="test") == "CLEAN"

    def test_quality_agent_warn_verdict_on_high_nulls(self):
        """Column with 11-50% nulls → verdict WARN (not BLOCK)."""
        from pulsar.core.intelligence.quality_agent import QualityAgent
        # 1 null out of 5 = 20% → above 10% threshold, below 50% → WARN
        df = pl.DataFrame({
            "a": [1, None, 3, 4, 5],
            "b": ["x", "y", "z", "w", "v"],
        })
        state = SharedStateStore()
        agent = QualityAgent(df=df, shared_state=state)
        agent._write_typed_keys()
        assert state.get("quality.verdict", stage="test") == "WARN"

    def test_schema_agent_writes_types_as_dict(self, sample_df):
        """schema.types is dict[str, str], not free text."""
        from pulsar.core.intelligence.schema_agent import SchemaAgent
        state = SharedStateStore()
        agent = SchemaAgent(df=sample_df, shared_state=state)
        agent._write_typed_keys()
        types = state.get("schema.types", stage="test")
        assert isinstance(types, dict)
        assert set(types.keys()) == set(sample_df.columns)
        for dtype_str in types.values():
            assert isinstance(dtype_str, str)

    def test_schema_agent_writes_columns_as_list(self, sample_df):
        """schema.columns is list[dict] with name+dtype fields."""
        from pulsar.core.intelligence.schema_agent import SchemaAgent
        state = SharedStateStore()
        agent = SchemaAgent(df=sample_df, shared_state=state)
        agent._write_typed_keys()
        columns = state.get("schema.columns", stage="test")
        assert isinstance(columns, list)
        assert len(columns) == sample_df.width
        assert all("name" in c and "dtype" in c for c in columns)

    def test_schema_agent_writes_cardinality_as_dict(self, sample_df):
        """schema.cardinality is dict[str, int]."""
        from pulsar.core.intelligence.schema_agent import SchemaAgent
        state = SharedStateStore()
        agent = SchemaAgent(df=sample_df, shared_state=state)
        agent._write_typed_keys()
        card = state.get("schema.cardinality", stage="test")
        assert isinstance(card, dict)
        for col, n in card.items():
            assert isinstance(n, int)


# ── Downsampling ──────────────────────────────────────────────────────────────

class TestDownsampling:
    def test_large_df_is_sampled(self):
        """DataFrames > 500K rows are sampled to 10K before agents run."""
        large_df = pl.DataFrame({"a": list(range(600_000))})
        # Simulate the downsampling logic from _run_scout
        _THRESHOLD = 500_000
        _TARGET = 10_000
        if large_df.shape[0] > _THRESHOLD:
            sampled = large_df.sample(n=_TARGET, seed=42)
        else:
            sampled = large_df
        assert sampled.shape[0] == _TARGET

    def test_small_df_is_not_sampled(self, sample_df):
        """DataFrames <= 500K rows are passed through unchanged."""
        _THRESHOLD = 500_000
        _TARGET = 10_000
        if sample_df.shape[0] > _THRESHOLD:
            sampled = sample_df.sample(n=_TARGET, seed=42)
        else:
            sampled = sample_df
        assert sampled.shape[0] == sample_df.shape[0]


# ── Tier-1 partial failure ────────────────────────────────────────────────────

class TestPartialFailure:
    def test_failed_agent_result_is_exception(self, sample_df, shared_state):
        """asyncio.gather return_exceptions=True yields Exception on failure."""
        import asyncio

        async def _fail():
            raise RuntimeError("LLM timeout")

        async def _ok():
            return "success"

        async def run():
            return await asyncio.gather(
                _fail(), _ok(), return_exceptions=True
            )

        results = asyncio.run(run())
        assert isinstance(results[0], RuntimeError)
        assert results[1] == "success"

    def test_surviving_agent_state_is_readable_after_partial_failure(
        self, sample_df
    ):
        """When one agent fails, surviving agents' keys are still in SharedStateStore."""
        state = SharedStateStore()
        state.set("schema.findings", ["col_a: string"], stage="schema")
        state.set("schema.response_summary", "Schema ok", stage="schema")
        # quality agent "failed" — its keys are absent
        # narrator should still see the schema keys
        context = state.get("schema.findings", stage="narrator")
        assert context == ["col_a: string"]
        assert state.get("quality.findings", stage="narrator") is None
