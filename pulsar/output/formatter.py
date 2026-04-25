# pulsar/output/formatter.py

from typing import Dict, Any, List


def _format_header() -> str:
    """Return the top header with box drawing"""
    return """╔════════════════════════════════════════════════════════════════╗
║                    PULSAR PROFILE REPORT                      ║
╚════════════════════════════════════════════════════════════════╝
"""


def _format_dataset_info(profile: Dict[str, Any], duration_ms: float = 0) -> str:
    """Format dataset info section with name, row count, column count"""
    dataset_name = profile.get("dataset_name", "unknown")
    row_count = profile.get("row_count", 0)
    col_count = profile.get("column_count", 0)
    
    duration_s = duration_ms / 1000
    
    return f"""📊 Dataset: {dataset_name}
   Rows: {row_count:,} | Columns: {col_count} | Profile Time: {duration_s:.2f}s
"""


def _format_column_numeric(col_data: Dict[str, Any]) -> str:
    """Format numeric column statistics"""
    stats = col_data.get("numeric_stats", {})
    
    min_val = stats.get('min')
    max_val = stats.get('max')
    mean_val = stats.get('mean')
    std_val = stats.get('std', 0)
    p25 = stats.get('p25')
    p50 = stats.get('p50')
    p75 = stats.get('p75')
    
    line = f"    ├─ Min: {min_val} | Max: {max_val} | Mean: {mean_val} | Std: {std_val:.2f}\n"
    line += f"    ├─ P25: {p25} | P50: {p50} | P75: {p75}\n"
    
    return line


def _format_column_categorical(col_data: Dict[str, Any]) -> str:
    """Format categorical column with top values"""
    stats = col_data.get("categorical_stats", {})
    top_k = stats.get("top_k", [])
    
    if not top_k:
        return "    ├─ Top Values: (none)\n"
    
    line = "    ├─ Top Values:\n"
    for item in top_k[:5]:
        value = item.get("value", "")
        count = item.get("count", 0)
        line += f"    │  • {value} ({count})\n"
    
    return line


def _format_column_datetime(col_data: Dict[str, Any]) -> str:
    """Format datetime column statistics"""
    stats = col_data.get("datetime_stats", {})
    
    min_val = stats.get('min')
    max_val = stats.get('max')
    
    if min_val and max_val:
        line = f"    ├─ Range: {min_val} to {max_val}\n"
    else:
        line = "    ├─ Range: (all nulls)\n"
    
    return line


def _format_single_column(col_name: str, col_data: Dict[str, Any], index: int) -> str:
    """Format a single column's complete information"""
    dtype = col_data.get("dtype", "")
    completeness = col_data.get("completeness", 0)
    null_count = col_data.get("null_count", 0)
    non_null_count = col_data.get("non_null_count", 0)
    uniqueness = col_data.get("uniqueness", 0)
    distinct_count = col_data.get("distinct_count", 0)
    samples = col_data.get("sample_values", [])
    
    output = f"│ {index}. {col_name} ({dtype})\n"
    
    # Completeness with warning if nulls exist
    warning = " ⚠️" if null_count > 0 else ""
    total_count = non_null_count + null_count
    output += f"│    ├─ Completeness: {completeness*100:.1f}% ({non_null_count}/{total_count} non-null){warning}\n"
    if null_count > 0:
        output += f"│    │                                      {null_count} null\n"
    
    # Uniqueness
    output += f"│    ├─ Uniqueness: {uniqueness*100:.1f}% ({distinct_count} distinct)\n"
    
    # Type-specific statistics
    if "numeric_stats" in col_data:
        output += _format_column_numeric(col_data)
    elif "categorical_stats" in col_data:
        output += _format_column_categorical(col_data)
    elif "datetime_stats" in col_data:
        output += _format_column_datetime(col_data)
    
    # Sample values
    sample_str = str(samples)[:50]  # Truncate if too long
    output += f"│    └─ Sample: {sample_str}\n"
    output += "│\n"
    
    return output


def _format_columns_section(profile: Dict[str, Any]) -> str:
    """Format all columns section"""
    output = "\n┌──────────────────────────────────────────────────────────────┐\n"
    output += "│ COLUMN DETAILS                                               │\n"
    output += "├──────────────────────────────────────────────────────────────┤\n"
    
    columns = profile.get("columns", {})
    for idx, (col_name, col_data) in enumerate(columns.items(), 1):
        output += _format_single_column(col_name, col_data, idx)
    
    output += "└──────────────────────────────────────────────────────────────┘\n"
    
    return output


def _format_summary_section(profile: Dict[str, Any]) -> str:
    """Format summary statistics section"""
    output = "\n┌──────────────────────────────────────────────────────────────┐\n"
    output += "│ SUMMARY STATISTICS                                           │\n"
    output += "├──────────────────────────────────────────────────────────────┤\n"
    
    columns = profile.get("columns", {})
    
    # Calculate overall completeness and uniqueness
    completeness_vals = [col.get("completeness", 0) for col in columns.values()]
    avg_completeness = (sum(completeness_vals) / len(completeness_vals) * 100) if completeness_vals else 0
    
    uniqueness_vals = [col.get("uniqueness", 0) for col in columns.values()]
    avg_uniqueness = (sum(uniqueness_vals) / len(uniqueness_vals) * 100) if uniqueness_vals else 0
    
    # Count column types
    numeric_count = sum(1 for col in columns.values() if "numeric_stats" in col)
    categorical_count = sum(1 for col in columns.values() if "categorical_stats" in col)
    datetime_count = sum(1 for col in columns.values() if "datetime_stats" in col)
    
    # Identify columns with nulls
    null_columns = [name for name, col in columns.items() if col.get("null_count", 0) > 0]
    null_columns_str = ", ".join(null_columns) if null_columns else "None"
    
    output += f"│ Overall Completeness:  {avg_completeness:.1f}%\n"
    output += f"│ Overall Uniqueness:    {avg_uniqueness:.1f}%\n"
    output += f"│ Columns with Nulls:    {len(null_columns)}  ({null_columns_str})\n"
    output += f"│ Numeric Columns:       {numeric_count}\n"
    output += f"│ String Columns:        {categorical_count}\n"
    output += f"│ DateTime Columns:      {datetime_count}\n"
    output += "└──────────────────────────────────────────────────────────────┘\n"
    
    return output


def format_profile_json(profile: Dict[str, Any]) -> str:
    """
    Return profile as JSON string.
    
    Args:
        profile: Profile dictionary from profiler.py
        
    Returns:
        JSON string representation
    """
    import json
    return json.dumps(profile, indent=2, default=str)


def format_profile_csv(profile: Dict[str, Any]) -> str:
    """
    Return profile as CSV with column statistics.
    
    Args:
        profile: Profile dictionary from profiler.py
        
    Returns:
        CSV string with one row per column
    """
    import csv
    import io
    
    columns = profile.get("columns", {})
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow([
        "Column",
        "Type",
        "Completeness",
        "Uniqueness",
        "Distinct Count",
        "Null Count",
        "Non-Null Count"
    ])
    
    # Data rows
    for col_name, col_data in columns.items():
        writer.writerow([
            col_name,
            col_data.get("dtype"),
            f"{col_data.get('completeness', 0)*100:.1f}%",
            f"{col_data.get('uniqueness', 0)*100:.1f}%",
            col_data.get("distinct_count"),
            col_data.get("null_count"),
            col_data.get("non_null_count"),
        ])
    
    return output.getvalue()


def format_profile_output(profile: Dict[str, Any], duration_ms: float = 0, verbose: bool = False) -> str:
    """
    Format the complete profile output into a structured, readable report.
    
    Args:
        profile: Profile dictionary from profiler.profile_dataset()
        duration_ms: Total time taken to profile in milliseconds
        
    Returns:
        Formatted string ready to print to terminal
        
    Example:
        >>> profile = profile_dataset(lf, path="data.csv")
        >>> output = format_profile_output(profile, duration_ms=450)
        >>> print(output)
    """
    output = ""
    
    # 1. Header
    output += _format_header()
    
    # 2. Dataset info
    output += _format_dataset_info(profile, duration_ms)
    
    # 3. Column details
    output += _format_columns_section(profile)
    
    # 4. Summary statistics
    output += _format_summary_section(profile)
    
    # 5. Footer with timing
    output += f"\n⏱️  Profile completed in {duration_ms/1000:.2f}s\n"
    
    return output


def format_profile_json(profile: Dict[str, Any]) -> str:
    """
    Return profile as JSON string.
    
    Args:
        profile: Profile dictionary
        
    Returns:
        JSON string representation
    """
    import json
    return json.dumps(profile, indent=2, default=str)