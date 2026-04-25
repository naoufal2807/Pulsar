# pulsar/logging_config.py

import logging
import sys
from datetime import datetime


def setup_logging(verbose: bool = False, log_file: str = None) -> logging.Logger:
    """
    Configure logging for Pulsar.
    
    Args:
        verbose: If True, set to DEBUG level, else INFO
        log_file: Optional file path to write logs to
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger("pulsar")
    
    # Clear existing handlers
    logger.handlers = []
    
    # Set level
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    
    # Format
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler (always)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            logger.info(f"Logging to file: {log_file}")
        except Exception as e:
            logger.warning(f"Could not create log file {log_file}: {e}")
    
    return logger


def get_logger(name: str = "pulsar") -> logging.Logger:
    """Get or create a logger instance."""
    return logging.getLogger(name)