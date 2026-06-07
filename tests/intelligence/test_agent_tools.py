# tests/intelligence/test_agent_tools.py
"""Tests for Agent tool-calling capabilities."""

import pytest
import polars as pl

from pulsar.core.intelligence.agent import Agent
from pulsar.core.intelligence.tools import (
    ToolRegistry, ToolDefinition, ToolParameter,
    create_default_registry,
    compute_statistics, check_data_quality,
    detect_outliers, analyze_correlation,
)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'value': [10.5, 20.3, 15.2, 100.0, 22.1],
        'category': ['A', 'B', 'A', 'C', 'B'],
        'score': [85, 92, 78, 88, 95],
    })


class TestToolRegistry:
    """Test ToolRegistry functionality."""

    def test_registry_creation(self):
        """Test creating a tool registry."""
        registry = ToolRegistry()
        assert len(registry.tools) == 0

    def test_tool_registration(self, sample_df):
        """Test registering a tool."""
        registry = create_default_registry(sample_df)

        assert 'compute_statistics' in registry.tools
        assert 'check_data_quality' in registry.tools
        assert 'detect_outliers' in registry.tools
        assert len(registry.tools) == 6

    def test_get_tool(self, sample_df):
        """Test retrieving a tool."""
        registry = create_default_registry(sample_df)

        tool = registry.get_tool('compute_statistics')
        assert tool is not None
        assert tool.name == 'compute_statistics'

    def test_get_tool_schemas(self, sample_df):
        """Test getting tool schemas for LLM."""
        registry = create_default_registry(sample_df)

        schemas = registry.get_schemas()
        assert len(schemas) == 6

        # Check schema structure
        schema = schemas[0]
        assert 'name' in schema
        assert 'description' in schema
        assert 'parameters' in schema


class TestDataAnalysisTools:
    """Test individual data analysis tools."""

    def test_compute_statistics_numeric(self, sample_df):
        """Test computing statistics for numeric column."""
        result = compute_statistics(sample_df, 'value')

        assert result['column'] == 'value'
        assert result['type'] == 'numeric'
        assert result['count'] == 5
        assert result['mean'] > 0
        assert result['std'] > 0

    def test_compute_statistics_categorical(self, sample_df):
        """Test computing statistics for categorical column."""
        result = compute_statistics(sample_df, 'category')

        assert result['column'] == 'category'
        assert result['type'] == 'non-numeric'
        assert result['distinct_count'] == 3

    def test_check_data_quality(self, sample_df):
        """Test data quality check."""
        result = check_data_quality(sample_df, 'value')

        assert result['column'] == 'value'
        assert result['total_rows'] == 5
        assert result['null_count'] == 0
        assert result['null_percent'] == 0

    def test_detect_outliers(self, sample_df):
        """Test outlier detection."""
        result = detect_outliers(sample_df, 'value')

        assert result['column'] == 'value'
        # 100.0 should be detected as outlier
        assert result['outlier_count'] > 0
        assert 100.0 in result['sample_outliers']

    def test_analyze_correlation(self, sample_df):
        """Test correlation analysis."""
        result = analyze_correlation(sample_df, 'value', 'score')

        assert result['column1'] == 'value'
        assert result['column2'] == 'score'
        assert 'correlation' in result
        assert 'strength' in result
        assert 'direction' in result


class TestAgentToolCalling:
    """Test Agent tool-calling capabilities."""

    def test_agent_with_tools_disabled(self, sample_df):
        """Test agent can be created without tools."""
        agent = Agent(df=sample_df, tools_enabled=False)

        assert agent.tools_enabled is False
        assert agent.tool_registry is None

    def test_agent_with_tools_enabled(self, sample_df):
        """Test agent initializes with tools."""
        agent = Agent(df=sample_df, tools_enabled=True)

        assert agent.tools_enabled is True
        assert agent.tool_registry is not None
        assert len(agent.tool_registry.tools) == 6

    def test_parse_tool_args(self, sample_df):
        """Test parsing tool arguments."""
        agent = Agent(df=sample_df, tools_enabled=True)

        # Test various argument formats
        args = agent._parse_tool_args('column=value, limit=10')
        assert args['column'] == 'value'
        assert args['limit'] == 10

        args = agent._parse_tool_args('col1="my column", col2=score')
        assert args['col1'] == 'my column'
        assert args['col2'] == 'score'

    def test_execute_tool_calls(self, sample_df):
        """Test executing tool calls from LLM response."""
        agent = Agent(df=sample_df, tools_enabled=True)

        # Simulate LLM response with tool call
        response = "Let me analyze the data. [TOOL: compute_statistics(column=value)]"
        results = agent._execute_tool_calls(response)

        assert results is not None
        assert 'compute_statistics' in results
        assert results['compute_statistics']['column'] == 'value'

    def test_execute_multiple_tool_calls(self, sample_df):
        """Test executing multiple tool calls."""
        agent = Agent(df=sample_df, tools_enabled=True)

        response = (
            "Let me analyze this. "
            "[TOOL: compute_statistics(column=value)] "
            "[TOOL: check_data_quality(column=category)]"
        )
        results = agent._execute_tool_calls(response)

        assert len(results) == 2
        assert 'compute_statistics' in results
        assert 'check_data_quality' in results

    def test_health_check_with_tools(self, sample_df):
        """Test health check reports tool status."""
        agent = Agent(df=sample_df, tools_enabled=True)

        health = agent.health_check()
        assert health['tools_enabled'] is True
        assert health['tools_available'] == 6

    def test_agent_think_with_tools_fallback(self, sample_df):
        """Test agent think with tools enabled but provider unavailable."""
        agent = Agent(df=sample_df, tools_enabled=True)
        agent.provider_available = False

        response = agent.think("Analyze the data")

        assert len(agent.memory) == 2  # user + assistant
        assert "unavailable" in response.lower()
