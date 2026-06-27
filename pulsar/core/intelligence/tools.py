# pulsar/core/intelligence/tools.py
"""
Tools: ToolRegistry infrastructure + backward-compatible re-exports.

Domain tool functions now live in:
  schema_tools.py  — describe_dataset, infer_domain, identify_key_entities, extract_key_metrics, describe_patterns
  quality_tools.py — check_data_quality, detect_outliers, explain_outliers
  stats_tools.py   — compute_statistics, analyze_correlation, get_top_values,
                     analyze_concentration, analyze_distribution_skewness,
                     analyze_variability, analyze_relationships, find_top_performers

create_default_registry() is kept here for backward compatibility;
it will be removed on Day 5 once each agent has its own _create_registry().
"""

from typing import Any, Dict, List, Optional, Callable, Tuple
import logging
from dataclasses import dataclass, asdict
import json

import polars as pl

from pulsar.core.intelligence.tool_result_cache import ToolResultCache
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

logger = logging.getLogger(__name__)


@dataclass
class ToolParameter:
    """Parameter definition for a tool."""
    name: str
    type: str  # 'string', 'number', 'boolean', 'object'
    description: str
    required: bool = True


@dataclass
class ToolDefinition:
    """Definition of a tool the Agent can call."""
    name: str
    description: str
    parameters: List[ToolParameter]
    function: Callable


class ToolRegistry:
    """Registry of tools available to the Agent with Layer 3 compression."""

    # Threshold for storing results to disk (5KB)
    RESULT_CACHE_THRESHOLD = 5000

    def __init__(self):
        self.tools: Dict[str, ToolDefinition] = {}
        self.cache: Dict[tuple, Any] = {}  # In-memory cache for small results
        self.result_cache = ToolResultCache()  # Layer 3: Disk cache for large results

    def register(self, tool_def: ToolDefinition) -> None:
        """Register a tool."""
        self.tools[tool_def.name] = tool_def
        logger.debug(f"Registered tool: {tool_def.name}")

    def get_tool(self, name: str) -> Optional[ToolDefinition]:
        """Get a tool by name."""
        return self.tools.get(name)

    def get_schemas(self) -> List[Dict[str, Any]]:
        """Get schemas for all tools (for LLM)."""
        schemas = []
        for tool in self.tools.values():
            schemas.append({
                'name': tool.name,
                'description': tool.description,
                'parameters': [asdict(p) for p in tool.parameters],
            })
        return schemas

    def call(self, tool_name: str, use_cache: bool = True, **kwargs) -> Any:
        """
        Call a tool by name with optional caching (Layer 3 compression).

        For large results (>5KB):
        - Store full result to disk via ToolResultCache
        - Cache summary in memory
        - Return summary to agent

        For small results (<5KB):
        - Keep in memory cache (backward compatible)
        """
        tool = self.get_tool(tool_name)
        if not tool:
            raise ValueError(f"Tool not found: {tool_name}")

        # Check cache
        if use_cache:
            cache_key = (tool_name, tuple(sorted(kwargs.items())))
            if cache_key in self.cache:
                logger.debug(f"Cache hit for {tool_name}")
                return self.cache[cache_key]

        logger.debug(f"Calling tool: {tool_name}")
        result = tool.function(**kwargs)
        result_size = len(json.dumps(result, default=str))
        logger.debug(f"Tool result size: {result_size} bytes")

        # Layer 3: Cache large results to disk (>5KB)
        if result_size > self.RESULT_CACHE_THRESHOLD:
            try:
                result_id, summary = self.result_cache.store_result(
                    tool_name, kwargs, result
                )
                # Cache the summary (small, ~200 chars)
                if use_cache:
                    cache_key = (tool_name, tuple(sorted(kwargs.items())))
                    cached_value = {
                        "summary": summary,
                        "result_id": result_id,
                        "original_size": result_size,
                    }
                    self.cache[cache_key] = cached_value
                    logger.info(
                        f"Cached large result to disk: {tool_name} "
                        f"({result_size} bytes → summary {len(summary)} chars)"
                    )
                # Return summary instead of full result
                return summary
            except Exception as e:
                logger.error(f"Layer 3 caching failed: {e}, returning full result")
                # Fallback: return full result if caching fails
        else:
            # Small results: use in-memory cache as before
            if use_cache:
                cache_key = (tool_name, tuple(sorted(kwargs.items())))
                self.cache[cache_key] = result

        return result

    def clear_cache(self) -> None:
        """Clear the tool result cache."""
        self.cache.clear()
        logger.debug("Tool cache cleared")


def create_default_registry(df: pl.DataFrame) -> ToolRegistry:
    """Create a registry with standard tools."""
    registry = ToolRegistry()

    registry.register(ToolDefinition(
        name='compute_statistics',
        description='Compute statistical metrics (mean, median, std, min, max) for a numeric column',
        parameters=[ToolParameter('column', 'string', 'Column name to analyze', required=True)],
        function=lambda column: compute_statistics(df, column),
    ))

    registry.register(ToolDefinition(
        name='check_data_quality',
        description='Check data quality metrics including null count, distinctness, duplicates',
        parameters=[ToolParameter('column', 'string', 'Column name to check', required=True)],
        function=lambda column: check_data_quality(df, column),
    ))

    registry.register(ToolDefinition(
        name='detect_outliers',
        description='Detect outliers using IQR method for numeric columns',
        parameters=[ToolParameter('column', 'string', 'Column name to analyze', required=True)],
        function=lambda column: detect_outliers(df, column),
    ))

    registry.register(ToolDefinition(
        name='analyze_correlation',
        description='Analyze correlation between two numeric columns',
        parameters=[
            ToolParameter('col1', 'string', 'First column name', required=True),
            ToolParameter('col2', 'string', 'Second column name', required=True),
        ],
        function=lambda col1, col2: analyze_correlation(df, col1, col2),
    ))

    registry.register(ToolDefinition(
        name='describe_dataset',
        description='Get overall dataset information (rows, columns, data types)',
        parameters=[],
        function=lambda: describe_dataset(df),
    ))

    registry.register(ToolDefinition(
        name='get_top_values',
        description='Get top N values for a column with their counts',
        parameters=[
            ToolParameter('column', 'string', 'Column name', required=True),
            ToolParameter('limit', 'number', 'Number of top values to return', required=False),
        ],
        function=lambda column, limit=10: get_top_values(df, column, limit),
    ))

    # Intelligence tools
    registry.register(ToolDefinition(
        name='infer_domain',
        description='Determine what business domain/industry this dataset represents',
        parameters=[],
        function=lambda: infer_domain(df),
    ))

    registry.register(ToolDefinition(
        name='identify_key_entities',
        description='Identify main entities and key terms in the dataset',
        parameters=[],
        function=lambda: identify_key_entities(df),
    ))

    registry.register(ToolDefinition(
        name='extract_key_metrics',
        description='Extract important numeric metrics from the dataset',
        parameters=[],
        function=lambda: extract_key_metrics(df),
    ))

    registry.register(ToolDefinition(
        name='describe_patterns',
        description='Describe discovered patterns in the dataset',
        parameters=[],
        function=lambda: describe_patterns(df),
    ))

    registry.register(ToolDefinition(
        name='find_top_performers',
        description='Find top performing entities by various metrics',
        parameters=[],
        function=lambda: find_top_performers(df),
    ))

    registry.register(ToolDefinition(
        name='explain_outliers',
        description='Explain what outliers mean and their business significance',
        parameters=[],
        function=lambda: explain_outliers(df),
    ))

    # Business analysis tools
    registry.register(ToolDefinition(
        name='analyze_concentration',
        description='Analyze market concentration using Herfindahl index',
        parameters=[],
        function=lambda: analyze_concentration(df),
    ))

    registry.register(ToolDefinition(
        name='analyze_distribution_skewness',
        description='Analyze distribution skewness (right-skewed, left-skewed, normal)',
        parameters=[],
        function=lambda: analyze_distribution_skewness(df),
    ))

    registry.register(ToolDefinition(
        name='analyze_variability',
        description='Analyze data consistency and variability',
        parameters=[],
        function=lambda: analyze_variability(df),
    ))

    registry.register(ToolDefinition(
        name='analyze_relationships',
        description='Analyze relationships and correlations between columns',
        parameters=[],
        function=lambda: analyze_relationships(df),
    ))

    return registry
