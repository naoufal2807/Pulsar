# pulsar/logging_config.py

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
import uuid


# Global session ID
SESSION_ID = str(uuid.uuid4())[:8]


def setup_logging(
    log_dir: str = "logs",
    level: int = logging.INFO,
    session_id: str = None
) -> str:
    """
    Setup file and console logging with session ID
    
    Args:
        log_dir: Directory to store log files
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        session_id: Unique session identifier (auto-generated if None)
    
    Returns:
        Path to log file
    
    Example:
        log_file = setup_logging(level=logging.DEBUG)
        logger = get_logger("my_module")
    """
    
    global SESSION_ID
    if session_id:
        SESSION_ID = session_id
    
    # Create logs directory
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Generate log file name with timestamp and session ID
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"pulsar_{timestamp}_{SESSION_ID}.log"
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(sessionId)s | %(name)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler (verbose, includes everything)
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # File gets everything
    file_handler.setFormatter(formatter)
    
    # Console handler (less verbose)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        fmt='%(levelname)s | %(name)s | %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Root captures everything
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Add handlers
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Add session ID to log records
    logging.LogRecord.sessionId = SESSION_ID
    old_factory = logging.getLogRecordFactory()
    
    def log_record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.sessionId = SESSION_ID
        return record
    
    logging.setLogRecordFactory(log_record_factory)
    
    root_logger.info(f"Logging initialized | Session: {SESSION_ID} | File: {log_file}")
    
    return str(log_file)


def get_logger(name: str) -> logging.Logger:
    """
    Get logger for a specific module
    
    Args:
        name: Module name (usually __name__)
    
    Returns:
        Logger instance
    
    Example:
        logger = get_logger("pulsar.quality.rules")
        logger.info("Something happened")
    """
    return logging.getLogger(name)


def get_session_id() -> str:
    """Get current session ID"""
    return SESSION_ID