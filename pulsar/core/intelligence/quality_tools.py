# pulsar/core/intelligence/quality_tools.py
"""Quality tools: data reliability (nulls, outliers, integrity)."""

from typing import Any, Dict, List, Optional
import logging

import polars as pl

logger = logging.getLogger(__name__)


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


def explain_outliers(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None) -> List[str]:
    """Explain what outliers mean and their business significance."""
    from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
    if patterns is None:
        patterns = {}
    gen = IntelligenceGenerator(df, patterns)
    return gen._explain_outliers()
