# pulsar/core/profiling/metrics.py

import re
from typing import Any, Dict, List, Optional
import polars as pl
import math


# ==================== NUMERIC METRICS ====================

def calculate_skewness(series: pl.Series) -> Optional[float]:
    """
    Calculate skewness of a numeric series.
    Skewness measures the asymmetry of the distribution.
    
    Returns:
        float: Skewness value (-3 to 3 typically)
        - Negative: left-skewed (tail on left)
        - 0: symmetric
        - Positive: right-skewed (tail on right)
    """
    if series.len() < 3:
        return None
    
    try:
        values = series.drop_nulls().to_list()
        if len(values) < 3:
            return None
        
        n = len(values)
        mean = sum(values) / n
        std = (sum((x - mean) ** 2 for x in values) / n) ** 0.5
        
        if std == 0:
            return None
        
        skew = sum((x - mean) ** 3 for x in values) / (n * (std ** 3))
        return float(skew)
    except Exception:
        return None


def calculate_kurtosis(series: pl.Series) -> Optional[float]:
    """
    Calculate kurtosis of a numeric series.
    Kurtosis measures the "tailedness" of the distribution.
    
    Returns:
        float: Kurtosis value
        - 0: normal distribution
        - Positive: heavier tails (more outliers)
        - Negative: lighter tails (fewer outliers)
    """
    if series.len() < 4:
        return None
    
    try:
        values = series.drop_nulls().to_list()
        if len(values) < 4:
            return None
        
        n = len(values)
        mean = sum(values) / n
        std = (sum((x - mean) ** 2 for x in values) / n) ** 0.5
        
        if std == 0:
            return None
        
        kurt = sum((x - mean) ** 4 for x in values) / (n * (std ** 4)) - 3
        return float(kurt)
    except Exception:
        return None


def detect_outliers_iqr(series: pl.Series) -> Dict[str, Any]:
    """
    Detect outliers using Interquartile Range (IQR) method.
    
    Outliers are values:
    - Less than Q1 - 1.5 * IQR
    - Greater than Q3 + 1.5 * IQR
    
    Returns:
        Dict with:
        - outlier_count: number of outliers detected
        - outlier_percentage: % of data that are outliers
        - lower_bound: lower threshold
        - upper_bound: upper threshold
    """
    if series.len() < 4:
        return {"outlier_count": 0, "outlier_percentage": 0.0}
    
    try:
        values = series.drop_nulls().to_list()
        if len(values) < 4:
            return {"outlier_count": 0, "outlier_percentage": 0.0}
        
        # Calculate quartiles
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        
        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1
        
        if iqr == 0:
            return {"outlier_count": 0, "outlier_percentage": 0.0}
        
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = [v for v in values if v < lower_bound or v > upper_bound]
        outlier_count = len(outliers)
        outlier_percentage = (outlier_count / len(values)) * 100 if len(values) > 0 else 0
        
        return {
            "method": "IQR",
            "outlier_count": outlier_count,
            "outlier_percentage": round(outlier_percentage, 2),
            "lower_bound": round(lower_bound, 2),
            "upper_bound": round(upper_bound, 2),
        }
    except Exception:
        return {"outlier_count": 0, "outlier_percentage": 0.0}


def detect_outliers_zscore(series: pl.Series, threshold: float = 3.0) -> Dict[str, Any]:
    """
    Detect outliers using Z-score method.
    
    Outliers are values with |z-score| > threshold (default 3).
    
    Returns:
        Dict with outlier statistics
    """
    if series.len() < 2:
        return {"outlier_count": 0, "outlier_percentage": 0.0}
    
    try:
        values = series.drop_nulls().to_list()
        if len(values) < 2:
            return {"outlier_count": 0, "outlier_percentage": 0.0}
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std = math.sqrt(variance)
        
        if std == 0:
            return {"outlier_count": 0, "outlier_percentage": 0.0}
        
        outliers = [v for v in values if abs((v - mean) / std) > threshold]
        outlier_count = len(outliers)
        outlier_percentage = (outlier_count / len(values)) * 100 if len(values) > 0 else 0
        
        return {
            "method": "Z-score",
            "threshold": threshold,
            "outlier_count": outlier_count,
            "outlier_percentage": round(outlier_percentage, 2),
        }
    except Exception:
        return {"outlier_count": 0, "outlier_percentage": 0.0}


def calculate_iqr_stats(series: pl.Series) -> Dict[str, Any]:
    """
    Calculate IQR (Interquartile Range) statistics.
    
    Returns:
        Dict with Q1, IQR, Q3 values
    """
    if series.len() < 4:
        return {}
    
    try:
        values = series.drop_nulls().to_list()
        if len(values) < 4:
            return {}
        
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        
        q1_idx = n // 4
        q3_idx = (3 * n) // 4
        
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1
        
        return {
            "q1": round(float(q1), 2),
            "q3": round(float(q3), 2),
            "iqr": round(iqr, 2),
        }
    except Exception:
        return {}


# ==================== STRING METRICS ====================

def analyze_string_patterns(series: pl.Series) -> Dict[str, Any]:
    """
    Analyze patterns and characteristics of string data.
    
    Returns:
        Dict with string statistics:
        - avg_length, min_length, max_length
        - uppercase_percentage, lowercase_percentage
        - special_chars_percentage
        - detected_formats (email, url, phone, etc.)
    """
    try:
        values = series.drop_nulls().to_list()
        values = [str(v) for v in values if v is not None]
        
        if len(values) == 0:
            return {
                "avg_length": 0,
                "min_length": 0,
                "max_length": 0,
                "uppercase_percentage": 0.0,
                "lowercase_percentage": 0.0,
                "special_chars_percentage": 0.0,
            }
        
        lengths = [len(v) for v in values]
        avg_length = sum(lengths) / len(lengths) if lengths else 0
        min_length = min(lengths) if lengths else 0
        max_length = max(lengths) if lengths else 0
        
        # Calculate character type percentages
        uppercase_count = sum(1 for v in values if any(c.isupper() for c in v))
        lowercase_count = sum(1 for v in values if any(c.islower() for c in v))
        special_chars_count = sum(1 for v in values if any(not c.isalnum() and c != ' ' for c in v))
        
        uppercase_pct = (uppercase_count / len(values)) * 100 if values else 0
        lowercase_pct = (lowercase_count / len(values)) * 100 if values else 0
        special_pct = (special_chars_count / len(values)) * 100 if values else 0
        
        # Detect formats
        formats = detect_string_formats(values)
        
        return {
            "avg_length": round(avg_length, 2),
            "min_length": min_length,
            "max_length": max_length,
            "uppercase_percentage": round(uppercase_pct, 1),
            "lowercase_percentage": round(lowercase_pct, 1),
            "special_chars_percentage": round(special_pct, 1),
            "detected_formats": formats,
        }
    except Exception:
        return {}


def detect_string_formats(values: List[str]) -> Dict[str, float]:
    """
    Detect common string formats in a list of values.
    
    Returns:
        Dict with format names and match percentages
    """
    if not values:
        return {}
    
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
    
    results = {}
    for format_name, pattern in patterns.items():
        matches = sum(1 for v in values if re.match(pattern, str(v)))
        match_percentage = (matches / len(values)) * 100 if values else 0
        if match_percentage > 0:
            results[format_name] = round(match_percentage, 1)
    
    return results


# ==================== TEMPORAL METRICS ====================

def analyze_date_range(series: pl.Series) -> Dict[str, Any]:
    """
    Analyze date/temporal columns.
    
    Returns:
        Dict with date statistics:
        - min_date, max_date
        - date_span (days between)
        - freshness (days since most recent)
        - distribution (% from last 30/90/365 days)
    """
    try:
        from datetime import datetime, timedelta
        
        values = series.drop_nulls().to_list()
        
        if len(values) == 0:
            return {}
        
        # Convert to datetime if needed
        dates = []
        for v in values:
            if isinstance(v, str):
                try:
                    dates.append(datetime.fromisoformat(v))
                except:
                    try:
                        dates.append(datetime.strptime(v, "%Y-%m-%d"))
                    except:
                        pass
            else:
                dates.append(v)
        
        if len(dates) == 0:
            return {}
        
        min_date = min(dates)
        max_date = max(dates)
        date_span = (max_date - min_date).days
        
        # Freshness: days since most recent
        today = datetime.now()
        freshness = (today - max_date).days
        
        # Distribution
        last_30 = sum(1 for d in dates if (today - d).days <= 30)
        last_90 = sum(1 for d in dates if (today - d).days <= 90)
        last_365 = sum(1 for d in dates if (today - d).days <= 365)
        
        pct_30 = (last_30 / len(dates)) * 100 if dates else 0
        pct_90 = (last_90 / len(dates)) * 100 if dates else 0
        pct_365 = (last_365 / len(dates)) * 100 if dates else 0
        
        return {
            "min_date": str(min_date.date()),
            "max_date": str(max_date.date()),
            "date_span_days": date_span,
            "freshness_days": freshness,
            "distribution": {
                "last_30_days_percentage": round(pct_30, 1),
                "last_90_days_percentage": round(pct_90, 1),
                "last_365_days_percentage": round(pct_365, 1),
            }
        }
    except Exception:
        return {}


# ==================== CARDINALITY ====================

def calculate_cardinality_ratio(series: pl.Series) -> float:
    """
    Calculate cardinality ratio: distinct_count / total_count
    
    Returns:
        float: Ratio between 0 and 1
        - Close to 0: many duplicates (low cardinality)
        - Close to 1: mostly unique (high cardinality)
    """
    if series.len() == 0:
        return 0.0
    
    try:
        total = series.len()
        distinct = series.n_unique()
        ratio = distinct / total if total > 0 else 0
        return round(ratio, 4)
    except Exception:
        return 0.0