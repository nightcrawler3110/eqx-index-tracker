"""
Module: main.py

Description:
------------
Main CLI entry point for the EQX index pipeline. Supports step-by-step or complete
execution of the data ingestion, processing, validation, and export workflow.

Supported Steps:
----------------
- ingest_data            : Ingest stock & market data for the given date
- build_index            : Build equal-weighted index for the date
- compute_daily_metrics  : Compute daily index metrics (returns, drawdown, etc.)
- compute_summary_metrics: Compute multi-day summary stats (Sharpe, Sortino, etc.)
- validate_data          : Run data validations on index/metrics
- export_excel           : Export results to Excel at user-defined location
- run_all                : Execute full pipeline in sequence

Usage Example:
--------------
$ python eqx_runner.py --steps run_all --date 2024-06-25 --window 30 --excel_output_dir /path/to/output
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from src.config import Config
from src.logger import setup_logging
from src.data_ingestion import run_ingestion
from src.index_builder import build_index
from src.daily_metrics_calculator import compute_daily_metrics
from src.summary_metrics_calculator import compute_summary_metrics
from src.excel_exporter import export_to_excel
from src.data_validations import run_validations


def validate_date(date_str: str) -> datetime:
    """
    Validates that the input string is a valid date in YYYY-MM-DD format.

    Args:
        date_str (str): Input date string.

    Returns:
        datetime: Parsed datetime object.

    Raises:
        argparse.ArgumentTypeError: If the format is invalid.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: '{date_str}'. Use YYYY-MM-DD."
        )


def execute_step(step_name: str, func: Callable[[], None]) -> Optional[Exception]:
    """
    Executes a single pipeline step with logging and error handling.

    Args:
        step_name (str): Name of the step for logging.
        func (Callable): Function to execute.

    Returns:
        Optional[Exception]: Returns the exception if failed, else None.
    """
    logging.info(f"Starting step: {step_name}")
    try:
        func()
        logging.info(f"Completed step: {step_name}")
        return None
    except Exception as e:
        logging.exception(f"Step failed: {step_name} — {e}")
        return e


def main() -> None:
    """
    Main function to parse arguments, configure logging,
    and execute selected pipeline steps.
    """
    parser = argparse.ArgumentParser(description="Run EQX index pipeline")

    parser.add_argument(
        "--steps",
        nargs="+",
        required=True,
        choices=[
            "ingest_data",
            "build_index",
            "compute_daily_metrics",
            "compute_summary_metrics",
            "validate_data",
            "export_excel",
            "run_all",
        ],
        help="Pipeline steps to run",
    )

    parser.add_argument(
        "--date",
        type=validate_date,
        required=True,
        help="Date to run pipeline for (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--window",
        type=int,
        default=40,
        help="Lookback window in days (for summary metrics)",
    )

    parser.add_argument(
        "--excel_output_dir",
        type=str,
        help="Custom directory to export the Excel file. Defaults to Config.EXCEL_OUTPUT_DIR.",
    )

    args = parser.parse_args()

    # Initialize logging
    setup_logging(log_file=Config.INGESTION_LOG_FILE)
    logging.info("EQX Pipeline Initialized")

    run_date: str = args.date.strftime("%Y-%m-%d")
    os.environ["FETCH_DAYS"] = str(args.window)

    # Map each pipeline step to its execution function
    step_map: Dict[str, Callable[[], None]] = {
        "ingest_data": lambda: run_ingestion(date=run_date),
        "build_index": lambda: build_index(date=run_date),
        "compute_daily_metrics": lambda: compute_daily_metrics(date=run_date),
        "compute_summary_metrics": lambda: compute_summary_metrics(date=run_date),
        "validate_data": lambda: run_validations(date=run_date),
        "export_excel": lambda: export_to_excel(
            date=run_date, output_dir=args.excel_output_dir
        ),
    }

    # Determine steps to run
    steps_to_run: List[str] = (
        [
            "ingest_data",
            "build_index",
            "compute_daily_metrics",
            "compute_summary_metrics",
            "validate_data",
            "export_excel",
        ]
        if "run_all" in args.steps
        else args.steps
    )

    # Execute each selected step
    errors: List[Tuple[str, Exception]] = []
    for step in steps_to_run:
        func = step_map.get(step)
        if func:
            error = execute_step(step, func)
            if error:
                errors.append((step, error))
        else:
            logging.warning(f"Unknown pipeline step: {step}")

    # Final result logging
    if errors:
        logging.error("Pipeline completed with errors:")
        for step, err in errors:
            logging.error(f" - {step}: {err}")
        sys.exit(1)
    else:
        logging.info("Pipeline completed successfully.")
        sys.exit(0)


if __name__ == "__main__":
    main()
