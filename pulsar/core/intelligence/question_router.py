# pulsar/core/intelligence/question_router.py
"""
QuestionRouter: maps CLI analysis modes to ordered agent tiers.

Tier 1 agents run concurrently (no shared-state dependencies).
Tier 2 agents run after all tier-1 agents complete (read tier-1 results).
"""

from typing import Dict, List, Tuple

# Key: CLI mode name
# Value: dict with "tier1" (concurrent) and "tier2" (sequential after tier1)
ROUTES: Dict[str, Dict[str, List[str]]] = {
    # Fast path: schema + quality only (<60s on local Ollama)
    "scout": {
        "tier1": ["schema", "quality"],
        "tier2": ["narrator"],
    },
    # Full pipeline: all three tier-1 agents + narrator
    "full": {
        "tier1": ["schema", "quality", "stats"],
        "tier2": ["narrator"],
    },
    "schema": {
        "tier1": ["schema"],
        "tier2": [],
    },
    "quality": {
        "tier1": ["quality"],
        "tier2": [],
    },
    "stats": {
        "tier1": ["stats"],
        "tier2": [],
    },
    "narrator": {
        "tier1": ["schema", "quality", "stats"],
        "tier2": ["narrator"],
    },
}

_DEFAULT_MODE = "scout"


def get_tiers(mode: str) -> Tuple[List[str], List[str]]:
    """
    Return (tier1_agent_keys, tier2_agent_keys) for a CLI mode.

    Args:
        mode: CLI mode string (e.g. "scout", "schema", "quality")

    Returns:
        Tuple of (tier1 keys, tier2 keys); both are lists of AgentRegistry keys.

    Falls back to full "scout" route if mode is unknown.
    """
    route = ROUTES.get(mode, ROUTES[_DEFAULT_MODE])
    return route["tier1"], route["tier2"]


def list_modes() -> List[str]:
    """Return all supported mode names."""
    return list(ROUTES.keys())


TIER1_QUESTION: Dict[str, str] = {
    "schema": (
        "Analyze this dataset's structure in full. "
        "Call describe_dataset, infer_domain, identify_key_entities, "
        "extract_key_metrics, and describe_patterns. "
        "Write a complete summary of your findings."
    ),
    "quality": (
        "Assess data quality for all columns. "
        "For each column call check_data_quality and detect_outliers. "
        "Then call explain_outliers for the full dataset. "
        "Summarize quality issues and severity."
    ),
    "stats": (
        "Compute full statistics for this dataset. "
        "Call compute_statistics and get_top_values for numeric/categorical columns. "
        "Then call analyze_correlation, analyze_concentration, "
        "analyze_distribution_skewness, analyze_variability, "
        "analyze_relationships, and find_top_performers. "
        "Summarize the key statistical patterns."
    ),
}
