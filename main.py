"""
Module: main.py

Overview:
---------
This module serves as the entry point for the EQX index data pipeline. It orchestrates the sequential
execution of all core steps involved in building and analyzing a custom equal-weighted index of the
top 100 US stocks by market cap.

Pipeline Steps:
---------------
1. Ingest historical stock and market index data.
2. Build the equal-weighted index using ingested data.
3. Compute detailed performance metrics for the index.
4. Run data validations to ensure integrity and correctness.
5. Export final metrics and index values to Excel.

Logging:
--------
- Logs are written to the path defined in `Config.INGESTION_LOG_FILE`.
- Logs each step's start, completion, or failure.
- Collects and reports all errors at the end.

Usage:
------
Run this script as the main entry point:

    $ python main.py

Author: Shaily
"""

import logging
from typing import Optional, Callable

from src.config import Config
from src.logger import setup_logging

from src.data_ingestion import run_ingestion
from src.index_builder import build_index
from src.performance_metrics import compute_metrics
from src.excel_exporter import export_to_excel
from src.data_validations import run_validations


def execute_step(step_name: str, func: Callable[[], None]) -> Optional[Exception]:
    """
    Executes a pipeline step and logs its outcome.

    Args:
        step_name (str): Human-readable name of the pipeline step.
        func (Callable[[], None]): The step function to invoke.

    Returns:
        Optional[Exception]: Returns the exception if an error occurs, else None.
    """
    logging.info(f"Starting: {step_name}")
    try:
        func()
        logging.info(f"Completed: {step_name}\n")
        return None
    except Exception as e:
        logging.exception(f"Failed: {step_name} — {e}")
        return e


def main() -> None:
    """
    Main orchestrator for the EQX pipeline.

    Steps:
        - Step 1: Data ingestion (stocks + market index)
        - Step 2: Build the equal-weighted index
        - Step 3: Compute index performance metrics
        - Step 4: Run validations on index and metrics
        - Step 5: Export results to Excel for analysis

    Logs all step results and summarizes any errors.
    """
    setup_logging(log_file=Config.INGESTION_LOG_FILE)
    logging.info("EQX Pipeline Started")

    steps = [
        ("Step 1: Fetching and storing stock data", run_ingestion),
        ("Step 2: Building equal-weighted index", build_index),
        ("Step 3: Computing performance metrics", compute_metrics),
        ("Step 4: Running data validations", run_validations),
        ("Step 5: Exporting to Excel", export_to_excel),
    ]

    errors = []

    for name, func in steps:
        error = execute_step(name, func)
        if error:
            errors.append((name, error))

    if errors:
        logging.error("Pipeline completed with errors:")
        for name, err in errors:
            logging.error(f" - {name}: {err}")
    else:
        logging.info("Pipeline completed successfully with no errors.")


if __name__ == "__main__":
    main()
