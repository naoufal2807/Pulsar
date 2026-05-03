# pulsar/cli.py

import typer
from pathlib import Path
import json
from typing import Optional, Dict, Any

import polars as pl

from pulsar.logging_config import setup_logging, get_logger
from pulsar.core.ingestion.loader import load
from pulsar.core.quality.loader import load_rules_yaml
from pulsar.core.quality.validator import Validator
from pulsar.output.formatter import format_validation_output

app = typer.Typer(help="Pulsar - Data Quality CLI")
logger = get_logger("pulsar.cli")


def _format_profile_text(profile: Dict[str, Any], verbose: bool = False) -> str:
    """Format profile as text table."""
    lines = []
    lines.append("\n" + "="*80)
    lines.append(f"📊 Dataset: {profile['dataset_name']}")
    lines.append(f"   {profile['row_count']:,} rows | {profile['column_count']} columns")
    lines.append("="*80)
    
    for col_name, col_data in profile["columns"].items():
        lines.append(f"\nColumn: {col_name} ({col_data['dtype']})")
        lines.append(f"├─ Completeness: {col_data['completeness']*100:.1f}% ({col_data['non_null_count']}/{profile['row_count']})")
        lines.append(f"├─ Uniqueness: {col_data['uniqueness']*100:.1f}% ({col_data['distinct_count']} distinct)")
        
        # Numeric stats
        if "numeric_stats" in col_data:
            stats = col_data["numeric_stats"]
            if not stats.get("error"):
                lines.append(f"├─ Distribution: Min {stats.get('min')} | Max {stats.get('max')} | Mean {stats.get('mean'):.2f}")
                if verbose and "skewness" in stats:
                    lines.append(f"├─ Skewness: {stats['skewness']:.2f}")
                    lines.append(f"├─ Kurtosis: {stats['kurtosis']:.2f}")
                    if "outliers" in stats:
                        lines.append(f"├─ Outliers (IQR): {stats['outliers'].get('iqr_method', 0)}")
                lines.append(f"└─ Percentiles: P25: {stats.get('p25')} | P50: {stats.get('p50')} | P75: {stats.get('p75')}")
        
        # DateTime stats
        elif "datetime_stats" in col_data:
            stats = col_data["datetime_stats"]
            lines.append(f"├─ Min: {stats.get('min')}")
            lines.append(f"└─ Max: {stats.get('max')}")
        
        # Categorical stats
        elif "categorical_stats" in col_data:
            stats = col_data["categorical_stats"]
            if stats.get("top_k"):
                lines.append(f"├─ Top values:")
                for item in stats["top_k"][:5]:
                    lines.append(f"│  • {item['value']}: {item['count']}")
            if verbose and "string_patterns" in stats:
                patterns = stats["string_patterns"]
                lines.append(f"├─ Patterns:")
                for pattern, count in list(patterns.items())[:3]:
                    lines.append(f"│  • {pattern}: {count}")
            lines.append(f"└─ Samples: {', '.join(col_data['sample_values'][:3])}")
    
    lines.append("\n" + "="*80 + "\n")
    return "\n".join(lines)


@app.command()
def profile(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    columns: Optional[str] = typer.Option(None, help="Columns to profile (comma-separated)"),
    output: str = typer.Option("text", help="Output format: text, json, csv"),
    verbose: bool = typer.Option(False, help="Show advanced metrics"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Profile a dataset."""
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Profile command: file={file}, output={output}, verbose={verbose}")
    
    try:
        from pulsar.core.profiling.profiler import profile_dataset
        
        lf = load(file)
        logger.info(f"File loaded: {file}")
        
        # Profile the dataset
        profile_data = profile_dataset(lf, path=file, detailed=verbose)
        logger.info(f"Dataset profiled: {profile_data['row_count']} rows, {profile_data['column_count']} columns")
        
        # Filter columns if specified
        if columns:
            col_list = [c.strip() for c in columns.split(",")]
            profile_data["columns"] = {k: v for k, v in profile_data["columns"].items() if k in col_list}
            logger.info(f"Filtered to {len(profile_data['columns'])} columns")
        
        # Format output
        if output == "json":
            print(json.dumps(profile_data, indent=2, default=str))
        else:  # text
            formatted = _format_profile_text(profile_data, verbose=verbose)
            print(formatted)
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        logger.error(f"Profile error: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def validate(
    file: str = typer.Argument(..., help="Path to data file (CSV/Parquet)"),
    rules: str = typer.Option(..., help="Path to rules YAML file"),
    output: str = typer.Option("text", help="Output format: text, json"),
    verbose: bool = typer.Option(False, help="Verbose output with details"),
    columns: Optional[str] = typer.Option(None, help="Columns to validate (comma-separated)"),
    log_file: Optional[str] = typer.Option(None, help="Log file path"),
):
    """Validate a dataset against rules."""
    log_path = setup_logging(log_file or "logs")
    logger.info(f"Validate command: file={file}, rules={rules}, output={output}")
    
    try:
        # Load data
        logger.debug(f"Loading file: {file}")
        lf = load(file)
        logger.info(f"File loaded: {file}")
        
        # Load rules
        logger.debug(f"Loading rules: {rules}")
        rules_list = load_rules_yaml(rules)
        logger.info(f"Loaded {len(rules_list)} rules")
        
        if not rules_list:
            logger.warning("No rules to validate")
            print("⚠️  No rules defined")
            return
        
        # Filter columns if specified
        if columns:
            col_list = [c.strip() for c in columns.split(",")]
            rules_list = [r for r in rules_list if r.column in col_list]
            logger.info(f"Filtered to {len(rules_list)} rules for columns: {col_list}")
        
        # Run validation
        logger.debug("Starting validation")
        validator = Validator()
        results = validator.validate(lf, rules_list)
        logger.info("Validation complete")
        
        # Format output
        formatted = format_validation_output(results, output=output, verbose=verbose)
        print(formatted)
        
        # Log summary
        passed = sum(1 for r in results.values() if r.get("status") == "PASS")
        total = len(results)
        logger.info(f"Validation summary: {passed}/{total} rules passed")
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        typer.echo(f"❌ Error: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()