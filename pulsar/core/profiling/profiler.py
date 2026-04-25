# pulsar/core/profiling/profiler.py
"""
Core profiling engine for Pulsar.
Pure Polars, zero external dependencies, LazyFrame-first.

Detects signals and patterns in your data.
"""
import os
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass

import polars as pl
from pulsar.core.profiling.metrics import (
    calculate_skewness,
    calculate_kurtosis,
    detect_outliers_iqr,
    detect_outliers_zscore,
    calculate_iqr_stats,
    analyze_string_patterns,
    analyze_date_range,
    calculate_cardinality_ratio,
)


# ---- Configuration (embedded, no external imports) ----
MAX_ROWS_PROFILE = 1_000_000  # 1M rows per dataset
MAX_TOP_K = 10  # top 10 value counts
CHUNK_SIZE = 100_000  # rows per chunk for streaming


# ---- Simple dtype helpers ----

NUMERIC_DTYPES = {
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
}

def _is_numeric_dtype(dtype: pl.DataType) -> bool:
    """Check if dtype is numeric."""
    return str(dtype) in NUMERIC_DTYPES


def _is_datetime_dtype(dtype: pl.DataType) -> bool:
    """Check if dtype is datetime-like."""
    s = str(dtype)
    return s == "Date" or s.startswith("Datetime")


def _is_categorical_dtype(dtype: pl.DataType) -> bool:
    """Check if dtype is categorical/string."""
    s = str(dtype)
    return s in ("Utf8", "String", "Categorical")


# ---- Loading helpers ----

def _load_lazy(path: str) -> pl.LazyFrame:
    """
    Detect file type and return LazyFrame.
    Supports: parquet, csv.
    Raises ValueError if unsupported.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    
    lower = path.lower()
    try:
        if lower.endswith((".parquet", ".pq")):
            return pl.scan_parquet(path)
        elif lower.endswith(".csv"):
            # Infer schema from first 10k rows, handle quoted fields
            return pl.scan_csv(
                path,
                infer_schema_length=10_000,
                ignore_errors=True,
            )
        else:
            raise ValueError(
                f"Unsupported file type: {path}. "
                f"Supported: .parquet, .csv"
            )
    except Exception as e:
        raise ValueError(
            f"Failed to load {path}: {str(e)}"
        ) from e


# ---- Profiling helpers ----

def _profile_numeric(series: pl.Series, detailed: bool = False) -> Dict[str, Any]:
    """
    Profile numeric column with distribution and outlier analysis.
    
    Args:
        series: Numeric column to profile
        detailed: If True, include skewness, kurtosis, outliers (for --verbose)
    """
    if series.len() == 0:
        return {
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "p25": None,
            "p50": None,
            "p75": None,
        }

    try:
        stats = {
            "min": float(series.min()),
            "max": float(series.max()),
            "mean": float(series.mean()),
            "std": float(series.std()) if series.len() > 1 else 0.0,
            "p25": float(series.quantile(0.25, interpolation="nearest")),
            "p50": float(series.quantile(0.50, interpolation="nearest")),
            "p75": float(series.quantile(0.75, interpolation="nearest")),
            "p90": float(series.quantile(0.90, interpolation="nearest")),
            "p99": float(series.quantile(0.99, interpolation="nearest")),
        }
        
        # Add advanced metrics if detailed
        if detailed:
            skewness = calculate_skewness(series)
            kurtosis = calculate_kurtosis(series)
            outliers_iqr = detect_outliers_iqr(series)
            outliers_zscore = detect_outliers_zscore(series)
            iqr_stats = calculate_iqr_stats(series)
            
            stats.update({
                "skewness": skewness,
                "kurtosis": kurtosis,
                "outliers": {
                    "iqr_method": outliers_iqr,
                    "zscore_method": outliers_zscore,
                },
                "iqr_stats": iqr_stats,
            })
        
        return stats
        
    except Exception as e:
        # Handle NaN, inf, or other numeric edge cases
        return {
            "min": None,
            "max": None,
            "mean": None,
            "std": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p90": None,
            "p99": None,
            "error": str(e),
        }


def _profile_categorical(
    series: pl.Series, top_k: int = MAX_TOP_K, detailed: bool = False
) -> Dict[str, Any]:
    """
    Profile categorical column: top-k value counts and patterns.
    
    Args:
        series: Categorical/string column
        top_k: Number of top values to show
        detailed: If True, include string patterns (for --verbose)
    """
    if series.len() == 0:
        return {"top_k": []}

    try:
        vc = (
            series
            .value_counts()
            .sort("count", descending=True)
            .head(top_k)
        )

        # Correct column order from value_counts output
        top_values = [
            {
                "value": str(row["values"]),  # Convert to string for JSON safety
                "count": int(row["count"]),
            }
            for row in vc.iter_rows(named=True)
        ]
        
        stats = {"top_k": top_values}
        
        # Add string patterns if detailed
        if detailed:
            string_patterns = analyze_string_patterns(series)
            stats["string_patterns"] = string_patterns
        
        return stats
        
    except Exception as e:
        return {"top_k": [], "error": str(e)}


def _sample_values(series: pl.Series, max_samples: int = 5) -> List[Any]:
    """Extract unique non-null sample values for display."""
    try:
        samples = (
            series
            .drop_nulls()
            .unique()
            .head(max_samples)
            .to_list()
        )
        # Convert to JSON-safe strings
        return [str(v) for v in samples]
    except Exception:
        return []


# ---- Main profiler ----

@dataclass
class ProfileStats:
    """Holds profiling metadata for benchmarking."""
    total_rows: int
    total_columns: int
    profile_rows: int
    duration_ms: float


def profile_dataset(
    lf: pl.LazyFrame,
    path: str = None,
    max_rows: int = MAX_ROWS_PROFILE,
    detailed: bool = False,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> Dict[str, Any]:
    """
    Profile a dataset end-to-end.

    Detects signals: completeness, uniqueness, distributions, top values.
    
    With detailed=True, also includes: skewness, kurtosis, outliers, 
    string patterns, date ranges.

    Args:
        lf: LazyFrame to profile
        path: File path (for display in output)
        max_rows: Max rows to profile (default 1M)
        detailed: If True, include advanced metrics (for --verbose)
        progress_callback: Optional callback(msg) for progress updates

    Returns:
        Profile dictionary with complete analysis

    Example:
        >>> lf = load("data.parquet")
        >>> profile = profile_dataset(lf, path="data.parquet")
        >>> print(profile["columns"]["email"])
    """
    start_time = time.time()
    
    def log_progress(msg: str):
        if progress_callback:
            progress_callback(msg)
    
    # Step 1: Limit rows
    log_progress(f"Sampling {max_rows:,} rows...")
    lf_limited = lf.head(max_rows)
    
    # Step 2: Collect into memory
    df = lf_limited.collect(streaming=True)
    
    total_rows = df.height
    total_columns = len(df.columns)
    
    log_progress(f"Profiling {total_columns} columns...")
    
    dataset_profile: Dict[str, Any] = {
        "dataset_name": os.path.basename(path) if path else "dataset",
        "path": path,
        "row_count": total_rows,
        "column_count": total_columns,
        "columns": {},
    }

    for idx, col in enumerate(df.columns):
        s = df[col]
        dtype = s.dtype

        # Compute basic metrics
        non_null_count = total_rows - s.null_count()
        null_count = s.null_count()
        completeness = (
            float(non_null_count / total_rows)
            if total_rows > 0
            else 0.0
        )
        distinct_count = int(s.n_unique())
        uniqueness = (
            float(distinct_count / non_null_count)
            if non_null_count > 0
            else 0.0
        )
        cardinality_ratio = calculate_cardinality_ratio(s)

        col_profile: Dict[str, Any] = {
            "position": idx,
            "dtype": str(dtype),
            "non_null_count": int(non_null_count),
            "null_count": int(null_count),
            "completeness": round(completeness, 4),
            "distinct_count": distinct_count,
            "uniqueness": round(uniqueness, 4),
            "cardinality_ratio": cardinality_ratio,
            "sample_values": _sample_values(s),
        }

        # Type-specific stats
        if _is_numeric_dtype(dtype):
            col_profile["numeric_stats"] = _profile_numeric(s, detailed=detailed)
        elif _is_datetime_dtype(dtype):
            if non_null_count > 0:
                col_profile["datetime_stats"] = {
                    "min": str(s.min()),
                    "max": str(s.max()),
                }
                if detailed:
                    date_analysis = analyze_date_range(s)
                    col_profile["datetime_stats"].update(date_analysis)
            else:
                col_profile["datetime_stats"] = {"min": None, "max": None}
        elif _is_categorical_dtype(dtype):
            col_profile["categorical_stats"] = _profile_categorical(s, detailed=detailed)

        dataset_profile["columns"][col] = col_profile

    duration_ms = (time.time() - start_time) * 1000
    log_progress(f"Done in {duration_ms:.0f}ms")
    
    return dataset_profile


# ---- Testing helpers ----

def profile_dataset_simple(lf: pl.LazyFrame, path: str = None) -> Dict[str, Any]:
    """
    Convenience wrapper: profile and return just the dict.
    Use for testing or when you don't need stats.
    """
    return profile_dataset(lf, path=path, detailed=False)