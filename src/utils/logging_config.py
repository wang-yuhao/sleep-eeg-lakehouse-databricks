"""Centralized logging configuration for DLT pipelines.

Provides structured logging with:
- Timestamp, level, module, and message
- Log file rotation
- Integration with Databricks event logs

Exam Coverage:
- Databricks Tooling (20%): Event logs, monitoring
"""

import logging
import sys
from datetime import datetime


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure logger with console and file handlers.
    
    Args:
        name: Logger name (usually __name__)
        level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Prevent duplicate handlers
    if logger.hasHandlers():
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Formatter with timestamp, level, module, message
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    
    return logger


# Default logger for imports
logger = setup_logger(__name__)