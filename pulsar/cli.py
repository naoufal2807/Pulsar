# pulsar/cli.py

import time
import typer
import logging
from pulsar.logging_config import setup_logging, get_logger
from pulsar.core.ingestion.loader import load
from pulsar.core.profiling.profiler import profile_dataset
from pulsar.output.formatter import format_profile_output, format_profile_json, format_profile_csv

app = typer.Typer()
logger = get_logger("pulsar.cli")


def parse_columns(columns_str: str | None) -> list[str] | None:
    """Parse comma-separated column names into a list"""
    if columns_str is None:
        return None
    
    logger.debug(f"Parsing columns: {columns_str}")
    
    # Split by comma and strip whitespace
    cols = [col.strip() for col in columns_str.split(",")]
    
    # Validate: no empty strings
    if any(col == "" for col in cols):
        raise ValueError("Invalid column names: empty strings found")
    
    logger.debug(f"Parsed columns: {cols}")
    return cols


@app.command()
def profile(
    file: str = typer.Argument(..., help="Path to data file (CSV or Parquet)"),
    output: str = typer.Option("text", help="Output format: text, json, or csv"),
    columns: str = typer.Option(None, help="Comma-separated list of columns to profile (e.g., user_id,name,age)"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show advanced metrics (skewness, kurtosis, outliers, patterns)"),
    log_file: str = typer.Option(None, "--log-file", help="Optional log file path"),
):
    """Profile a dataset and show completeness, uniqueness, and distribution statistics."""
    
    # Setup logging
    setup_logging(verbose=verbose, log_file=log_file)
    logger.info("=" * 80)
    logger.info("Pulsar Profile Started")
    logger.info(f"File: {file}")
    logger.info(f"Output: {output}")
    logger.info(f"Columns: {columns if columns else 'all'}")
    logger.info(f"Verbose: {verbose}")
    
    try:
        start_time = time.time()
        logger.debug("Starting profile_dataset process...")
        
        # 1. Load file
        logger.info(f"Loading file: {file}")
        lf = load(file)
        logger.info("File loaded successfully")
        
        # 2. Filter columns if specified
        selected_columns = None
        if columns:
            logger.info(f"Filtering to columns: {columns}")
            selected_columns = parse_columns(columns)
            lf = lf.select(selected_columns)
            logger.debug(f"Filtered to {len(selected_columns)} columns")
        
        # 3. Profile it (with detailed=True if verbose)
        logger.info("Starting data profiling...")
        profile_result = profile_dataset(lf, path=file, detailed=verbose)
        logger.info("Data profiling completed")
        
        # 4. Calculate duration
        duration_ms = (time.time() - start_time) * 1000
        logger.info(f"Profile completed in {duration_ms:.0f}ms")
        
        # 5. Format based on output type
        logger.debug(f"Formatting output as: {output}")
        if output.lower() == "json":
            formatted = format_profile_json(profile_result)
        elif output.lower() == "csv":
            formatted = format_profile_csv(profile_result)
        else:  # default: text
            formatted = format_profile_output(profile_result, duration_ms=duration_ms, verbose=verbose)
        
        logger.debug("Output formatted successfully")
        print(formatted)
        logger.info("=" * 80)
        
    except FileNotFoundError as e:
        logger.error(f"FileNotFoundError: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        raise typer.Exit(code=1)
        
    except ValueError as e:
        logger.error(f"ValueError: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        raise typer.Exit(code=1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        print(f"❌ Unexpected error: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()