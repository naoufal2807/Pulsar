#loader for the files

import polars as pl
import pathlib

def load(path: str) -> pl.LazyFrame:
    """
    Detect file type (CSV/Parquet)
    Validate file exists 
    Return LazyFrame (not materialized yet)
    Raise clear errors if file is corrupted 
   """
    file_path = pathlib.Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    # 2 supported formats parquet and csv 
    
    if file_path.suffix == ".parquet":
        try:
            return pl.scan_parquet(file_path)
        except Exception as e: 
            raise ValueError(f"Error reading {file_path}: {e}")
    elif file_path.suffix == ".csv":
        try: 
            return pl.scan_csv(file_path)
        except Exception as e: 
            raise ValueError(f"Error reading {file_path}: {e}")
    else:
        raise ValueError("Unsupported file format")