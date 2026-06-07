# pulsar/core/intelligence/small_world/learner.py
"""
Learner: Discover patterns from sample data.

The Learner takes a sample (typically 10% of data) and deeply understands its patterns:
- Formats (email, phone, URL, IP, date, UUID, credit card)
- Ranges (min, max, mean, median, std)
- Distributions (skewness, kurtosis, outliers)
- Relationships (correlations between columns)
- Patterns (seasonality, trends)

Output is a dictionary of discovered patterns that can be used by Expander
to validate and refine understanding against the full dataset.
"""

from typing import Any, Dict, List, Optional, Tuple
import logging
from datetime import datetime

import polars as pl

from pulsar.core.profiling.metrics import (
    calculate_skewness,
    calculate_kurtosis,
    detect_outliers_iqr,
    detect_outliers_zscore,
    analyze_string_patterns,
    analyze_date_range,
    calculate_cardinality_ratio,
)

logger = logging.getLogger(__name__)


class Learner:
    """
    Discover patterns from a sample of data.

    This is the second step in the Small World Framework:
    1. Isolator extracts a bounded sample (10%)
    2. Learner discovers deep patterns from that sample
    3. Expander validates and refines patterns layer-by-layer
    4. Contextualizer infers business meaning
    5. Analyzer generates understanding
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Learner.

        Args:
            config: Optional configuration dictionary
        """
        self.config = config or {}
        self.logger = logger

    def learn(self, sample: pl.DataFrame) -> Dict[str, Any]:
        """
        Discover all patterns from sample.

        Args:
            sample: DataFrame sample (typically 10% of full data)

        Returns:
            Dictionary with discovered patterns:
            {
                'formats': {...},
                'ranges': {...},
                'distributions': {...},
                'relationships': {...},
                'patterns': {...},
                'metadata': {...}
            }
        """
        if sample.is_empty():
            self.logger.warning("Empty sample provided to Learner")
            return {
                'formats': {},
                'ranges': {},
                'distributions': {},
                'relationships': {},
                'patterns': {},
                'metadata': {'sample_size': 0}
            }

        self.logger.info(
            f"Learning patterns from sample: {sample.shape[0]} rows, "
            f"{sample.shape[1]} columns"
        )

        patterns = {
            'formats': self.discover_formats(sample),
            'ranges': self.discover_ranges(sample),
            'distributions': self.discover_distributions(sample),
            'relationships': self.discover_relationships(sample),
            'patterns': self.discover_patterns(sample),
            'metadata': {
                'sample_size': sample.shape[0],
                'num_columns': sample.shape[1],
                'columns': sample.columns,
                'learned_at': datetime.now().isoformat(),
            }
        }

        return patterns

    def discover_formats(self, sample: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Discover string formats in sample columns.

        For each string/categorical column, detects:
        - Email, URL, phone (US/intl), IPv4, UUID, date, credit card patterns
        - Match ratios for each format
        - Examples matching each format

        Returns:
            {
                'column_name': {
                    'email': {
                        'match_ratio': 0.98,
                        'pattern': r'^[a-zA-Z0-9._%+-]+@...',
                        'examples': ['user@example.com', ...],
                        'count': 98
                    },
                    ...
                },
                ...
            }
        """
        formats = {}

        for col_name in sample.columns:
            col = sample[col_name]
            dtype_str = str(col.dtype)

            # Only analyze string/categorical columns
            if dtype_str not in ('Utf8', 'String', 'Categorical'):
                continue

            # Get string values
            try:
                values = col.drop_nulls().to_list()
                if not values:
                    continue

                string_values = [str(v) for v in values]

                # Analyze patterns
                string_stats = analyze_string_patterns(col)
                detected_formats = string_stats.get('detected_formats', {})

                # Extract examples for each detected format
                col_formats = {}
                for fmt_name, match_pct in detected_formats.items():
                    examples = self._get_format_examples(string_values, fmt_name, max_count=3)
                    col_formats[fmt_name] = {
                        'match_ratio': round(match_pct / 100, 3),
                        'match_percentage': match_pct,
                        'count': int(len(string_values) * (match_pct / 100)),
                        'examples': examples,
                    }

                if col_formats:
                    formats[col_name] = col_formats
                    self.logger.debug(
                        f"Column '{col_name}': Detected formats {list(col_formats.keys())}"
                    )

            except Exception as e:
                self.logger.debug(f"Error analyzing formats for column '{col_name}': {e}")

        return formats

    def discover_ranges(self, sample: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Discover numeric ranges in sample columns.

        For each numeric column, calculates:
        - Min, max, mean, median, std dev
        - Percentiles (p25, p50, p75, p90, p99)

        For temporal columns, calculates:
        - Date range (min, max)
        - Freshness (days since max)
        - Distribution (% in last 30/90/365 days)

        For categorical columns, calculates:
        - Cardinality ratio (unique/total)
        - Value counts

        Returns:
            {
                'numeric_column': {
                    'min': 0,
                    'max': 100,
                    'mean': 50.5,
                    'median': 50,
                    'std': 28.87,
                    'p25': 25, 'p50': 50, 'p75': 75, 'p90': 90, 'p99': 99
                },
                'date_column': {
                    'min_date': '2020-01-01',
                    'max_date': '2025-12-31',
                    'date_span_days': 2191,
                    'freshness_days': 123,
                    'distribution': {...}
                },
                'category_column': {
                    'cardinality_ratio': 0.85,
                    'unique_count': 850,
                    'total_count': 1000,
                    'top_values': [('value1', 100), ('value2', 95), ...]
                }
            }
        """
        ranges = {}

        for col_name in sample.columns:
            col = sample[col_name]
            dtype_str = str(col.dtype)

            try:
                # Numeric columns
                if dtype_str in ('Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64'):
                    numeric_series = col.drop_nulls()
                    if numeric_series.len() > 0:
                        ranges[col_name] = {
                            'type': 'numeric',
                            'min': float(numeric_series.min()),
                            'max': float(numeric_series.max()),
                            'mean': float(numeric_series.mean()),
                            'median': float(numeric_series.quantile(0.5)),
                            'std': float(numeric_series.std()) if numeric_series.len() > 1 else 0.0,
                            'p25': float(numeric_series.quantile(0.25)),
                            'p50': float(numeric_series.quantile(0.50)),
                            'p75': float(numeric_series.quantile(0.75)),
                            'p90': float(numeric_series.quantile(0.90)),
                            'p99': float(numeric_series.quantile(0.99)),
                        }

                # Date/temporal columns
                elif dtype_str in ('Date', 'Datetime') or 'Datetime' in dtype_str:
                    date_stats = analyze_date_range(col)
                    if date_stats:
                        ranges[col_name] = {
                            'type': 'temporal',
                            **date_stats
                        }

                # Categorical/string columns
                if dtype_str in ('Utf8', 'String', 'Categorical'):
                    non_null_col = col.drop_nulls()
                    if non_null_col.len() > 0:
                        cardinality = calculate_cardinality_ratio(col)
                        value_counts = non_null_col.value_counts(sort=True).head(10)

                        top_values = [
                            (str(row[0]), int(row[1]))
                            for row in value_counts.rows()
                        ]

                        ranges[col_name] = {
                            'type': 'categorical',
                            'cardinality_ratio': cardinality,
                            'unique_count': non_null_col.n_unique(),
                            'total_count': non_null_col.len(),
                            'top_values': top_values,
                        }

            except Exception as e:
                self.logger.debug(f"Error analyzing ranges for column '{col_name}': {e}")

        return ranges

    def discover_distributions(self, sample: pl.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Discover distribution characteristics in numeric columns.

        For each numeric column, calculates:
        - Skewness (symmetry of distribution)
        - Kurtosis (heaviness of tails / presence of outliers)
        - Outlier detection (IQR and Z-score methods)
        - IQR statistics

        Returns:
            {
                'numeric_column': {
                    'skewness': -0.45,  # negative = left-skewed
                    'kurtosis': 0.5,    # positive = heavy tails
                    'outliers_iqr': {
                        'method': 'IQR',
                        'outlier_count': 5,
                        'outlier_percentage': 0.5,
                        'lower_bound': 10.5,
                        'upper_bound': 89.5,
                    },
                    'outliers_zscore': {
                        'method': 'Z-score',
                        'threshold': 3.0,
                        'outlier_count': 2,
                        'outlier_percentage': 0.2,
                    },
                    'iqr_stats': {
                        'q1': 25.0,
                        'q3': 75.0,
                        'iqr': 50.0,
                    }
                },
                ...
            }
        """
        distributions = {}

        for col_name in sample.columns:
            col = sample[col_name]
            dtype_str = str(col.dtype)

            # Only analyze numeric columns
            if dtype_str not in ('Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64'):
                continue

            try:
                numeric_series = col.drop_nulls()
                if numeric_series.len() < 4:
                    continue

                skewness = calculate_skewness(numeric_series)
                kurtosis = calculate_kurtosis(numeric_series)
                outliers_iqr = detect_outliers_iqr(numeric_series)
                outliers_zscore = detect_outliers_zscore(numeric_series)

                dist_info = {}
                if skewness is not None:
                    dist_info['skewness'] = round(skewness, 3)
                if kurtosis is not None:
                    dist_info['kurtosis'] = round(kurtosis, 3)

                dist_info['outliers_iqr'] = outliers_iqr
                dist_info['outliers_zscore'] = outliers_zscore

                distributions[col_name] = dist_info
                self.logger.debug(
                    f"Column '{col_name}': Skewness={skewness:.3f}, "
                    f"Kurtosis={kurtosis:.3f}, Outliers={outliers_iqr.get('outlier_count', 0)}"
                )

            except Exception as e:
                self.logger.debug(f"Error analyzing distributions for column '{col_name}': {e}")

        return distributions

    def discover_relationships(self, sample: pl.DataFrame) -> Dict[str, List[Tuple[str, float]]]:
        """
        Discover relationships between numeric columns.

        Calculates Pearson correlation between numeric columns.

        Returns:
            {
                'column_a': [
                    ('column_b', 0.85),   # strong positive correlation
                    ('column_c', -0.42),  # moderate negative correlation
                ],
                ...
            }

        Only includes correlations with |r| > 0.1 (weak but notable).
        """
        relationships = {}

        try:
            # Get numeric columns only
            numeric_cols = []
            for col_name in sample.columns:
                dtype_str = str(sample[col_name].dtype)
                if dtype_str in ('Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64'):
                    numeric_cols.append(col_name)

            if len(numeric_cols) < 2:
                return relationships

            # Select only numeric columns
            numeric_df = sample.select(numeric_cols).drop_nulls()

            if numeric_df.is_empty():
                return relationships

            # Calculate correlations
            for i, col_a in enumerate(numeric_cols):
                correlations = []
                for col_b in numeric_cols[i+1:]:
                    try:
                        # Calculate Pearson correlation
                        series_a = numeric_df[col_a]
                        series_b = numeric_df[col_b]

                        if series_a.len() < 2 or series_b.len() < 2:
                            continue

                        # Manual Pearson correlation calculation
                        mean_a = series_a.mean()
                        mean_b = series_b.mean()

                        values_a = series_a.to_list()
                        values_b = series_b.to_list()

                        numerator = sum((a - mean_a) * (b - mean_b) for a, b in zip(values_a, values_b))
                        denom_a = sum((a - mean_a) ** 2 for a in values_a) ** 0.5
                        denom_b = sum((b - mean_b) ** 2 for b in values_b) ** 0.5

                        if denom_a == 0 or denom_b == 0:
                            continue

                        correlation = numerator / (denom_a * denom_b)

                        # Only include notable correlations
                        if abs(correlation) > 0.1:
                            correlations.append((col_b, round(correlation, 3)))

                    except Exception as e:
                        self.logger.debug(f"Error calculating correlation between {col_a} and {col_b}: {e}")

                if correlations:
                    # Sort by absolute correlation (strongest first)
                    correlations.sort(key=lambda x: abs(x[1]), reverse=True)
                    relationships[col_a] = correlations

        except Exception as e:
            self.logger.debug(f"Error discovering relationships: {e}")

        return relationships

    def discover_patterns(self, sample: pl.DataFrame) -> Dict[str, Any]:
        """
        Discover high-level patterns in data.

        For temporal columns, detects:
        - Seasonality (regular repeating patterns)
        - Trend (increasing or decreasing)

        For categorical columns, detects:
        - Dominance (one value >> others)
        - Distribution type (uniform, power law, etc.)

        Returns:
            {
                'temporal_patterns': {
                    'date_column': {
                        'has_seasonality': True,
                        'seasonality_period': 'monthly',
                        'trend': 'increasing',
                    }
                },
                'categorical_patterns': {
                    'category_column': {
                        'is_dominated': True,
                        'dominant_value': 'A',
                        'dominant_ratio': 0.85,
                        'distribution_type': 'power_law',
                    }
                },
                'data_quality_indicators': {
                    'has_duplicates': True,
                    'duplicate_ratio': 0.05,
                    'null_ratio_by_column': {...}
                }
            }
        """
        patterns = {
            'temporal_patterns': {},
            'categorical_patterns': {},
            'data_quality_indicators': {
                'has_duplicates': False,
                'duplicate_ratio': 0.0,
                'null_ratio_by_column': {},
            }
        }

        try:
            # Detect duplicates
            total_rows = sample.shape[0]
            if total_rows > 0:
                # Count unique rows using is_duplicated()
                duplicated = sample.is_duplicated().sum()
                patterns['data_quality_indicators']['has_duplicates'] = duplicated > 0
                patterns['data_quality_indicators']['duplicate_ratio'] = round(
                    duplicated / total_rows, 3
                )

            # Null ratios by column
            for col_name in sample.columns:
                null_count = sample[col_name].null_count()
                null_ratio = null_count / total_rows if total_rows > 0 else 0
                if null_ratio > 0:
                    patterns['data_quality_indicators']['null_ratio_by_column'][col_name] = round(
                        null_ratio, 3
                    )

            # Temporal patterns
            for col_name in sample.columns:
                col = sample[col_name]
                dtype_str = str(col.dtype)

                if dtype_str in ('Date', 'Datetime') or 'Datetime' in dtype_str:
                    try:
                        temporal_pattern = self._analyze_temporal_column(col)
                        if temporal_pattern:
                            patterns['temporal_patterns'][col_name] = temporal_pattern
                    except Exception as e:
                        self.logger.debug(f"Error analyzing temporal pattern for '{col_name}': {e}")

            # Categorical patterns
            for col_name in sample.columns:
                col = sample[col_name]
                dtype_str = str(col.dtype)

                if dtype_str in ('Utf8', 'String', 'Categorical'):
                    try:
                        cat_pattern = self._analyze_categorical_column(col)
                        if cat_pattern:
                            patterns['categorical_patterns'][col_name] = cat_pattern
                    except Exception as e:
                        self.logger.debug(f"Error analyzing categorical pattern for '{col_name}': {e}")

        except Exception as e:
            self.logger.debug(f"Error discovering patterns: {e}")

        return patterns

    # Helper methods

    def _get_format_examples(self, values: List[str], format_name: str, max_count: int = 3) -> List[str]:
        """Extract example values matching a specific format."""
        import re

        patterns = {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "url": r"^https?://[^\s]+$",
            "phone_us": r"^(\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$",
            "phone_intl": r"^\+[0-9]{1,3}[-.\s]?[0-9]+$",
            "ipv4": r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$",
            "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            "date_iso": r"^\d{4}-\d{2}-\d{2}$",
            "credit_card": r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$",
        }

        pattern = patterns.get(format_name)
        if not pattern:
            return []

        examples = []
        for v in values:
            if re.match(pattern, str(v)):
                examples.append(str(v))
                if len(examples) >= max_count:
                    break

        return examples

    def _analyze_temporal_column(self, col: pl.Series) -> Dict[str, Any]:
        """Analyze temporal column for patterns."""
        # Simple heuristic: check if values are mostly recent (last 365 days)
        # In a full implementation, this would detect seasonality, trend, etc.

        try:
            dates = col.drop_nulls().to_list()
            if len(dates) < 10:
                return {}

            # Very basic pattern detection
            pattern = {}

            # Check if we have a trend (simplified: compare first half vs second half)
            mid = len(dates) // 2
            first_half = dates[:mid]
            second_half = dates[mid:]

            if first_half and second_half:
                avg_first = sum(d.timestamp() if hasattr(d, 'timestamp') else d for d in first_half) / len(first_half)
                avg_second = sum(d.timestamp() if hasattr(d, 'timestamp') else d for d in second_half) / len(second_half)

                if avg_second > avg_first:
                    pattern['trend'] = 'increasing'
                elif avg_second < avg_first:
                    pattern['trend'] = 'decreasing'
                else:
                    pattern['trend'] = 'stable'

            return pattern

        except Exception:
            return {}

    def _analyze_categorical_column(self, col: pl.Series) -> Dict[str, Any]:
        """Analyze categorical column for patterns."""
        try:
            non_null = col.drop_nulls()
            if non_null.len() == 0:
                return {}

            # Value counts
            value_counts = non_null.value_counts(sort=True)
            rows = value_counts.rows()

            if not rows:
                return {}

            # Check for dominance (top value >> 50%)
            top_count = rows[0][1]
            total = non_null.len()
            dominant_ratio = top_count / total if total > 0 else 0

            pattern = {}
            if dominant_ratio > 0.5:
                pattern['is_dominated'] = True
                pattern['dominant_value'] = str(rows[0][0])
                pattern['dominant_ratio'] = round(dominant_ratio, 3)
            else:
                pattern['is_dominated'] = False

            # Simple distribution detection
            if len(rows) == 1:
                pattern['distribution_type'] = 'constant'
            elif dominant_ratio > 0.8:
                pattern['distribution_type'] = 'power_law'
            elif dominant_ratio > 0.5:
                pattern['distribution_type'] = 'skewed'
            else:
                pattern['distribution_type'] = 'uniform'

            return pattern

        except Exception:
            return {}
