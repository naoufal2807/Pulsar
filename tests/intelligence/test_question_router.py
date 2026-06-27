# tests/intelligence/test_question_router.py
"""Unit tests for QuestionRouter."""

import pytest

from pulsar.core.intelligence.question_router import (
    get_tiers,
    list_modes,
    ROUTES,
    TIER1_QUESTION,
)


class TestGetTiers:
    def test_scout_returns_three_tier1(self):
        tier1, tier2 = get_tiers("scout")
        assert set(tier1) == {"schema", "quality", "stats"}

    def test_scout_returns_narrator_tier2(self):
        tier1, tier2 = get_tiers("scout")
        assert tier2 == ["narrator"]

    def test_schema_mode_only(self):
        tier1, tier2 = get_tiers("schema")
        assert tier1 == ["schema"]
        assert tier2 == []

    def test_quality_mode_only(self):
        tier1, tier2 = get_tiers("quality")
        assert tier1 == ["quality"]
        assert tier2 == []

    def test_stats_mode_only(self):
        tier1, tier2 = get_tiers("stats")
        assert tier1 == ["stats"]
        assert tier2 == []

    def test_unknown_mode_falls_back_to_scout(self):
        tier1, tier2 = get_tiers("nonexistent_mode")
        assert set(tier1) == {"schema", "quality", "stats"}
        assert "narrator" in tier2

    def test_returns_tuple_of_two_lists(self):
        result = get_tiers("scout")
        assert isinstance(result, tuple)
        assert len(result) == 2
        tier1, tier2 = result
        assert isinstance(tier1, list)
        assert isinstance(tier2, list)


class TestListModes:
    def test_contains_scout(self):
        assert "scout" in list_modes()

    def test_contains_all_agent_modes(self):
        modes = list_modes()
        assert "schema" in modes
        assert "quality" in modes
        assert "stats" in modes
        assert "narrator" in modes

    def test_returns_list(self):
        assert isinstance(list_modes(), list)


class TestRoutes:
    def test_scout_route_structure(self):
        assert "tier1" in ROUTES["scout"]
        assert "tier2" in ROUTES["scout"]

    def test_no_narrator_in_tier1(self):
        for mode, route in ROUTES.items():
            assert "narrator" not in route["tier1"], (
                f"narrator should never be in tier1 (found in mode='{mode}')"
            )

    def test_tier2_only_has_narrator_or_empty(self):
        for mode, route in ROUTES.items():
            for key in route["tier2"]:
                assert key == "narrator", (
                    f"Only narrator allowed in tier2 (found '{key}' in mode='{mode}')"
                )


class TestTier1Questions:
    def test_has_question_for_each_tier1_key(self):
        for key in ("schema", "quality", "stats"):
            assert key in TIER1_QUESTION
            assert len(TIER1_QUESTION[key]) > 20

    def test_schema_question_mentions_tools(self):
        q = TIER1_QUESTION["schema"].lower()
        assert "describe_dataset" in q or "domain" in q

    def test_quality_question_mentions_tools(self):
        q = TIER1_QUESTION["quality"].lower()
        assert "quality" in q or "outlier" in q

    def test_stats_question_mentions_tools(self):
        q = TIER1_QUESTION["stats"].lower()
        assert "statistic" in q or "correlat" in q
