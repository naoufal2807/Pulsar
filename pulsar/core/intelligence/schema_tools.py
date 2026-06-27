# pulsar/core/intelligence/schema_tools.py
"""Schema tools: structural understanding of a dataset (shape, types, domain, entities)."""

from typing import Any, Dict, List, Optional
import logging

import polars as pl

logger = logging.getLogger(__name__)


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


def infer_domain(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None) -> str:
    """Infer what domain/industry this dataset represents."""
    from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
    if patterns is None:
        patterns = {}
    gen = IntelligenceGenerator(df, patterns)
    return gen._infer_domain()


def identify_key_entities(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None) -> Dict[str, List[str]]:
    """Identify main entities (key terms, values) in the dataset."""
    from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
    if patterns is None:
        patterns = {}
    gen = IntelligenceGenerator(df, patterns)
    return gen._identify_key_entities()


def extract_key_metrics(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Extract important numeric metrics from the dataset."""
    from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
    if patterns is None:
        patterns = {}
    gen = IntelligenceGenerator(df, patterns)
    return gen._extract_key_metrics()


def describe_patterns(df: pl.DataFrame, patterns: Optional[Dict[str, Any]] = None) -> List[str]:
    """Describe discovered patterns in the dataset."""
    from pulsar.core.intelligence.small_world.intelligence_generator import IntelligenceGenerator
    if patterns is None:
        patterns = {}
    gen = IntelligenceGenerator(df, patterns)
    return gen._describe_patterns()
