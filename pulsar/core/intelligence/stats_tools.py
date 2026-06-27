# pulsar/core/intelligence/stats_tools.py
"""Stats tools: numerical analysis (distributions, correlations, variability, top-N)."""

from typing import Any, Dict, List, Optional
import logging

import polars as pl

logger = logging.getLogger(__name__)


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


def analyze_correlation(df: pl.DataFrame, col1: str, col2: str) -> Dict[str, Any]:
    """Analyze correlation between two columns."""
    try:
        c1 = df[col1]
        c2 = df[col2]

        c1_type = df[col1].dtype
        c2_type = df[col2].dtype

        if c1_type not in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            return {'error': f'{col1} is not numeric', 'columns': [col1, col2]}
        if c2_type not in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            return {'error': f'{col2} is not numeric', 'columns': [col1, col2]}

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


def analyze_concentration(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None, intelligence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Analyze market concentration using Herfindahl index."""
    from pulsar.core.intelligence.small_world.deep_analyzer import DeepAnalyzer
    if patterns is None:
        patterns = {}
    if intelligence is None:
        intelligence = {}
    analyzer = DeepAnalyzer(df, patterns, intelligence)
    return analyzer.analyze_concentration()


def analyze_distribution_skewness(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None, intelligence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Analyze distribution skewness (right-skewed, left-skewed, normal)."""
    from pulsar.core.intelligence.small_world.deep_analyzer import DeepAnalyzer
    if patterns is None:
        patterns = {}
    if intelligence is None:
        intelligence = {}
    analyzer = DeepAnalyzer(df, patterns, intelligence)
    return analyzer.analyze_distribution_skewness()


def analyze_variability(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None, intelligence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Analyze data consistency and variability."""
    from pulsar.core.intelligence.small_world.deep_analyzer import DeepAnalyzer
    if patterns is None:
        patterns = {}
    if intelligence is None:
        intelligence = {}
    analyzer = DeepAnalyzer(df, patterns, intelligence)
    return analyzer.analyze_variability()


def analyze_relationships(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None, intelligence: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Analyze relationships and correlations between columns."""
    from pulsar.core.intelligence.small_world.deep_analyzer import DeepAnalyzer
    if patterns is None:
        patterns = {}
    if intelligence is None:
        intelligence = {}
    analyzer = DeepAnalyzer(df, patterns, intelligence)
    return analyzer.analyze_relationships()


def find_top_performers(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None) -> Dict[str, List[tuple]]:
    """Find top performing entities by various metrics."""
    from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
    if patterns is None:
        patterns = {}
    gen = IntelligenceGenerator(df, patterns)
    return gen._find_top_performers()
