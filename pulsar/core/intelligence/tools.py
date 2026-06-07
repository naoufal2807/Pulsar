# pulsar/core/intelligence/tools.py
"""
Tools: Functions the Agent can call for data analysis.

Provides a toolkit for the Agent to:
- Compute statistics
- Check data quality
- Detect patterns
- Analyze relationships
- Generate insights
"""

from typing import Any, Dict, List, Optional, Callable
import logging
from dataclasses import dataclass, asdict
import json

import polars as pl

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
    """Registry of tools available to the Agent."""

    def __init__(self):
        self.tools: Dict[str, ToolDefinition] = {}

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

    def call(self, tool_name: str, **kwargs) -> Any:
        """Call a tool by name."""
        tool = self.get_tool(tool_name)
        if not tool:
            raise ValueError(f"Tool not found: {tool_name}")

        logger.debug(f"Calling tool: {tool_name} with args: {kwargs}")
        result = tool.function(**kwargs)
        logger.debug(f"Tool result: {str(result)[:200]}")
        return result


# Data Analysis Tools

def compute_statistics(df: pl.DataFrame, column: str) -> Dict[str, Any]:
    """Compute statistics for a numeric column."""
    try:
        col_data = df[column]
        col_type = df[column].dtype

        if col_type in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            return {
                'column': column,
                'count': col_data.len(),
                'null_count': col_data.is_null().sum(),
                'mean': col_data.mean(),
                'median': col_data.median(),
                'std': col_data.std(),
                'min': col_data.min(),
                'max': col_data.max(),
                'type': 'numeric',
            }
        else:
            return {
                'column': column,
                'type': 'non-numeric',
                'dtype': str(col_type),
                'distinct_count': col_data.n_unique(),
            }
    except Exception as e:
        return {'error': str(e), 'column': column}


def check_data_quality(df: pl.DataFrame, column: str) -> Dict[str, Any]:
    """Check data quality metrics for a column."""
    try:
        col_data = df[column]
        total = col_data.len()

        return {
            'column': column,
            'total_rows': total,
            'null_count': col_data.is_null().sum(),
            'null_percent': (col_data.is_null().sum() / total * 100) if total > 0 else 0,
            'distinct_count': col_data.n_unique(),
            'distinctness_percent': (col_data.n_unique() / total * 100) if total > 0 else 0,
            'duplicate_rows': total - col_data.n_unique(),
        }
    except Exception as e:
        return {'error': str(e), 'column': column}


def detect_outliers(df: pl.DataFrame, column: str) -> Dict[str, Any]:
    """Detect outliers using IQR method."""
    try:
        col_data = df[column]
        col_type = df[column].dtype

        if col_type not in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            return {'column': column, 'type': 'non-numeric', 'outliers': []}

        q1 = col_data.quantile(0.25)
        q3 = col_data.quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers = col_data.filter(
            (col_data < lower_bound) | (col_data > upper_bound)
        ).to_list()

        return {
            'column': column,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'outlier_count': len(outliers),
            'outlier_percent': (len(outliers) / col_data.len() * 100) if col_data.len() > 0 else 0,
            'sample_outliers': outliers[:5],
        }
    except Exception as e:
        return {'error': str(e), 'column': column}


def analyze_correlation(df: pl.DataFrame, col1: str, col2: str) -> Dict[str, Any]:
    """Analyze correlation between two columns."""
    try:
        c1 = df[col1]
        c2 = df[col2]

        # Check if numeric
        c1_type = df[col1].dtype
        c2_type = df[col2].dtype

        if c1_type not in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            return {'error': f'{col1} is not numeric', 'columns': [col1, col2]}
        if c2_type not in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            return {'error': f'{col2} is not numeric', 'columns': [col1, col2]}

        # Calculate Pearson correlation manually
        import numpy as np

        c1_vals = c1.to_numpy()
        c2_vals = c2.to_numpy()

        corr = float(np.corrcoef(c1_vals, c2_vals)[0, 1])

        return {
            'column1': col1,
            'column2': col2,
            'correlation': corr,
            'strength': 'strong' if abs(corr) > 0.7 else 'moderate' if abs(corr) > 0.3 else 'weak',
            'direction': 'positive' if corr > 0 else 'negative',
        }
    except Exception as e:
        return {'error': str(e), 'columns': [col1, col2]}


def describe_dataset(df: pl.DataFrame) -> Dict[str, Any]:
    """Get overall dataset description."""
    try:
        return {
            'row_count': df.height,
            'column_count': df.width,
            'columns': df.columns,
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
            'memory_usage': df.estimated_size() / 1024 / 1024,  # MB
        }
    except Exception as e:
        return {'error': str(e)}


def get_top_values(df: pl.DataFrame, column: str, limit: int = 10) -> Dict[str, Any]:
    """Get top N values for a column."""
    try:
        top = df[column].value_counts().head(limit)

        return {
            'column': column,
            'top_values': [
                {'value': str(row[0]), 'count': int(row[1])}
                for row in top.to_dicts()
            ],
        }
    except Exception as e:
        return {'error': str(e), 'column': column}


# Create tool definitions

COMPUTE_STATISTICS_TOOL = ToolDefinition(
    name='compute_statistics',
    description='Compute statistical metrics (mean, median, std, min, max) for a numeric column',
    parameters=[
        ToolParameter('column', 'string', 'Column name to analyze', required=True),
    ],
    function=lambda df, column: compute_statistics(df, column),
)

CHECK_DATA_QUALITY_TOOL = ToolDefinition(
    name='check_data_quality',
    description='Check data quality metrics including null count, distinctness, duplicates',
    parameters=[
        ToolParameter('column', 'string', 'Column name to check', required=True),
    ],
    function=lambda df, column: check_data_quality(df, column),
)

DETECT_OUTLIERS_TOOL = ToolDefinition(
    name='detect_outliers',
    description='Detect outliers using IQR method for numeric columns',
    parameters=[
        ToolParameter('column', 'string', 'Column name to analyze', required=True),
    ],
    function=lambda df, column: detect_outliers(df, column),
)

ANALYZE_CORRELATION_TOOL = ToolDefinition(
    name='analyze_correlation',
    description='Analyze correlation between two numeric columns',
    parameters=[
        ToolParameter('col1', 'string', 'First column name', required=True),
        ToolParameter('col2', 'string', 'Second column name', required=True),
    ],
    function=lambda df, col1, col2: analyze_correlation(df, col1, col2),
)

DESCRIBE_DATASET_TOOL = ToolDefinition(
    name='describe_dataset',
    description='Get overall dataset information (rows, columns, data types)',
    parameters=[],
    function=lambda df: describe_dataset(df),
)

GET_TOP_VALUES_TOOL = ToolDefinition(
    name='get_top_values',
    description='Get top N values for a column with their counts',
    parameters=[
        ToolParameter('column', 'string', 'Column name', required=True),
        ToolParameter('limit', 'number', 'Number of top values to return', required=False),
    ],
    function=lambda df, column, limit=10: get_top_values(df, column, limit),
)


def create_default_registry(df: pl.DataFrame) -> ToolRegistry:
    """Create a registry with standard tools."""
    registry = ToolRegistry()

    # Wrap functions with df parameter
    wrapped_stats = ToolDefinition(
        name='compute_statistics',
        description=COMPUTE_STATISTICS_TOOL.description,
        parameters=COMPUTE_STATISTICS_TOOL.parameters,
        function=lambda column: compute_statistics(df, column),
    )

    wrapped_quality = ToolDefinition(
        name='check_data_quality',
        description=CHECK_DATA_QUALITY_TOOL.description,
        parameters=CHECK_DATA_QUALITY_TOOL.parameters,
        function=lambda column: check_data_quality(df, column),
    )

    wrapped_outliers = ToolDefinition(
        name='detect_outliers',
        description=DETECT_OUTLIERS_TOOL.description,
        parameters=DETECT_OUTLIERS_TOOL.parameters,
        function=lambda column: detect_outliers(df, column),
    )

    wrapped_corr = ToolDefinition(
        name='analyze_correlation',
        description=ANALYZE_CORRELATION_TOOL.description,
        parameters=ANALYZE_CORRELATION_TOOL.parameters,
        function=lambda col1, col2: analyze_correlation(df, col1, col2),
    )

    wrapped_describe = ToolDefinition(
        name='describe_dataset',
        description=DESCRIBE_DATASET_TOOL.description,
        parameters=DESCRIBE_DATASET_TOOL.parameters,
        function=lambda: describe_dataset(df),
    )

    wrapped_top = ToolDefinition(
        name='get_top_values',
        description=GET_TOP_VALUES_TOOL.description,
        parameters=GET_TOP_VALUES_TOOL.parameters,
        function=lambda column, limit=10: get_top_values(df, column, limit),
    )

    registry.register(wrapped_stats)
    registry.register(wrapped_quality)
    registry.register(wrapped_outliers)
    registry.register(wrapped_corr)
    registry.register(wrapped_describe)
    registry.register(wrapped_top)

    return registry
