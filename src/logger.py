"""
logger.py — Centralized Logging Setup

This module provides a reusable `setup_logging` function to configure consistent logging
across the EQX Index project. It supports per-module log file paths and logger names.

Features:
---------
- Dynamic logger names (to isolate logs per module)
- Configurable log file output
- File + console logging
- Prevents duplicate handlers on repeated imports
"""

import logging
from pathlib import Path
from typing import Optional, Union


def setup_logging(
    log_file: Optional[Union[str, Path]] = None,
    level: int = logging.INFO,
    logger_name: str = "eqx.default"
) -> logging.Logger:
    """
    Sets up and returns a logger configured with both file and console handlers.

    Args:
        log_file (str | Path | None): Full path to the log file. If None, logging only to console.
        level (int): Logging level. Default is logging.INFO.
        logger_name (str): Unique name for the logger. Prevents log contamination across modules.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Avoid adding multiple handlers if already set
    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        logger.info(f"Logging initialized. Output -> {log_path}")

    return logger
