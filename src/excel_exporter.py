"""
Module: excel_export

Description:
------------
This module handles exporting various index-related metrics and composition data
from a DuckDB database to a structured Excel workbook.

Exports include:
- Daily index performance (returns, value, etc.)
- Daily composition breakdown (exploded ticker columns)
- Day-over-day composition changes (added/removed tickers)
- Summary statistics of the index

Functions:
----------
- safe_split(): Robustly parses tickers from various formats.
- load_data_from_duckdb(): Loads required tables from DuckDB.
- transform_composition(): Converts list-like tickers into separate columns.
- compute_composition_changes(): Tracks changes in index composition.
- write_excel(): Writes all dataframes to Excel with separate sheets.
- export_to_excel(): Main orchestrator function with safe file handling.

logger:
--------
All steps and issues are logged using the configured logger.

Configuration:
--------------
- Input: Config.DUCKDB_FILE
- Output: Config.EXCEL_OUTPUT_FILE
- Logs: Config.EXCEL_EXPORT_LOG_FILE
"""

import os
import logging
from pathlib import Path
from typing import List, Any

import duckdb
import pandas as pd
import numpy as np

from src.config import Config
from src.logger import setup_logging

# --- Initialize logger ---
logger = setup_logging(Config.EXCEL_EXPORT_LOG_FILE, logger_name="eqx.excel_exporter")


def safe_split(x: Any) -> List[str]:
    """Safely split tickers from stringified lists or comma-separated strings."""
    try:
        if isinstance(x, (list, np.ndarray)):
            return list(x)
        elif isinstance(x, str):
            x = x.strip("[]").replace("'", "")
            return [item.strip() for item in x.split(",") if item.strip()]
        return []
    except Exception as e:
        logger.warning(f"safe_split failed for input: {x} | Error: {e}")
        return []


def load_data_from_duckdb() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch data from DuckDB for export."""
    conn = duckdb.connect(Config.DUCKDB_FILE)
    try:
        performance_df = conn.execute(
            """
            SELECT date, index_value, daily_return, cumulative_return
            FROM index_metrics
            ORDER BY date
        """
        ).fetch_df()

        composition_df = conn.execute(
            """
            SELECT date, tickers FROM index_metrics ORDER BY date
        """
        ).fetch_df()

        summary_df = conn.execute("SELECT * FROM summary_metrics").fetch_df()

        logger.info("Data loaded from DuckDB.")
        return performance_df, composition_df, summary_df
    finally:
        conn.close()


def transform_composition(composition_df: pd.DataFrame) -> pd.DataFrame:
    """Explode tickers column into separate columns per day."""
    composition_df = composition_df.copy()
    composition_df["tickers"] = composition_df["tickers"].apply(safe_split)

    exploded = composition_df["tickers"].apply(pd.Series)
    exploded.columns = [f"ticker_{i+1}" for i in range(exploded.shape[1])]
    return pd.concat([composition_df["date"], exploded], axis=1)


def compute_composition_changes(composition_df: pd.DataFrame) -> pd.DataFrame:
    """Compute added/removed tickers compared to previous day."""
    changes = []
    prev_set: set[str] = set()

    for _, row in composition_df.iterrows():
        current_set = set(safe_split(row["tickers"]))
        added = current_set - prev_set
        removed = prev_set - current_set
        intersection = current_set & prev_set

        changes.append(
            {
                "date": row["date"],
                "added": ",".join(sorted(added)),
                "removed": ",".join(sorted(removed)),
                "intersection_size": len(intersection),
            }
        )
        prev_set = current_set

    return pd.DataFrame(changes)


def write_excel(
    performance_df: pd.DataFrame,
    composition_final: pd.DataFrame,
    changes_df: pd.DataFrame,
    summary_df: pd.DataFrame,
) -> None:
    """Write multiple dataframes to a single Excel file."""
    with pd.ExcelWriter(Config.EXCEL_OUTPUT_FILE, engine="openpyxl") as writer:
        performance_df.to_excel(writer, sheet_name="index_performance", index=False)
        composition_final.to_excel(writer, sheet_name="daily_composition", index=False)
        changes_df.to_excel(writer, sheet_name="composition_changes", index=False)
        summary_df.to_excel(writer, sheet_name="summary_metrics", index=False)


def export_to_excel() -> None:
    """Main function to export index data to Excel."""
    logger.info("Starting Excel export process...")

    # Delete old export if exists
    try:
        if Path(Config.EXCEL_OUTPUT_FILE).exists():
            os.remove(Config.EXCEL_OUTPUT_FILE)
            logger.info(f"Deleted old Excel file: {Config.EXCEL_OUTPUT_FILE}")
    except Exception as e:
        logger.warning(f"Failed to delete old Excel file: {e}")

    if not Path(Config.DUCKDB_FILE).exists():
        logger.error(f"DuckDB file not found at {Config.DUCKDB_FILE}")
        return

    try:
        performance_df, composition_df, summary_df = load_data_from_duckdb()
        composition_final = transform_composition(composition_df)
        changes_df = compute_composition_changes(composition_df)
        write_excel(performance_df, composition_final, changes_df, summary_df)

        logger.info(f"Excel export successful: {Config.EXCEL_OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Export failed: {e}")
