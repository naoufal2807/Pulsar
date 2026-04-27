# pulsar/cli.py

import typer
from pathlib import Path
import json
from typing import Optional

import polars as pl

from pulsar.logging_config import setup_logging, get_logger
from pulsar.core.ingestion.loader import load
from pulsar.core.quality.loader import load_rules_yaml
from pulsar.core.quality.validator import Validator
from pulsar.output.formatter import format_validation_output

app = typer.Typer(help="Pulsar - Data Quality CLI")
logger = get_logger("pulsar.cli")


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
        lf = load(file)
        logger.info(f"File loaded: {file}")
        
        if output == "text":
            print(f"✅ Profiled: {file}")
        elif output == "json":
            print(json.dumps({"status": "ok", "file": file}))
        
    except Exception as e:
        logger.error(f"Profile failed: {e}", exc_info=True)
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
