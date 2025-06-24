"""
Module: data_validation

Description:
------------
This module provides essential data validation functions and a runner to check the integrity
of key tables in a DuckDB database.

It validates against:
- Null values in critical columns
- Non-positive values (e.g., close price <= 0)
- Abnormal price spikes (>10x day-over-day)

Design:
-------
- Each validation logs its findings and saves detailed issue rows as CSVs.
- The final validation summary report is saved as a CSV file.
- Fails gracefully on missing tables or errors.

Functions:
----------
- validate_no_nulls(): Check for missing values.
- validate_positive_values(): Check for non-positive numeric fields.
- validate_price_spikes(): Detect extreme price changes.
- run_for_table(): Run all validations on a specific table.
- run_validations(): Orchestrates validations across all key tables.

Types:
------
- ValidationRecord: Tuple[str, str, str, int, str] representing (table, issue, column, count, path).

logger:
--------
All steps, warnings, and validation issues are logged using the configured logger.

Output:
-------
- Detailed issue rows saved in `Config.DETAILED_ISSUES_DIR`.
- Summary report saved to `Config.VALIDATION_LOG`.
"""

import logging
from pathlib import Path
from typing import Callable, List, Tuple

import duckdb
import pandas as pd
import numpy as np

from src.config import Config
from src.logger import setup_logging

# --- Initialize logger ---
logger = setup_logging(Config.VALIDATION_LOG_FILE, logger_name="eqx.data_validation")

ValidationRecord = Tuple[str, str, str, int, str]

# --- Validation Functions ---


def validate_no_nulls(
    df: pd.DataFrame, table_name: str, columns: List[str]
) -> List[ValidationRecord]:
    records = []
    for col in columns:
        if df[col].isnull().any():
            bad_rows = df[df[col].isnull()]
            path = Config.DETAILED_ISSUES_DIR / f"{table_name}__nulls__{col}.csv"
            bad_rows.to_csv(path, index=False)
            records.append((table_name, "Null values", col, len(bad_rows), str(path)))
    return records


def validate_positive_values(
    df: pd.DataFrame, table_name: str, columns: List[str]
) -> List[ValidationRecord]:
    records = []
    for col in columns:
        bad_rows = df[df[col] <= 0]
        if not bad_rows.empty:
            path = Config.DETAILED_ISSUES_DIR / f"{table_name}__non_positive__{col}.csv"
            bad_rows.to_csv(path, index=False)
            records.append(
                (table_name, "Non-positive values", col, len(bad_rows), str(path))
            )
    return records


def validate_price_spikes(df: pd.DataFrame, table_name: str) -> List[ValidationRecord]:
    if {"ticker", "close", "date"}.issubset(df.columns):
        df = df.sort_values(["ticker", "date"])
        df["prev_close"] = df.groupby("ticker")["close"].shift(1)
        df["change_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"]
        bad_rows = df[df["change_pct"].abs() > 10]
        if not bad_rows.empty:
            path = Config.DETAILED_ISSUES_DIR / f"{table_name}__price_spike_gt_10x.csv"
            bad_rows.to_csv(path, index=False)
            return [
                (
                    table_name,
                    "Price change >10x vs previous day",
                    "close",
                    len(bad_rows),
                    str(path),
                )
            ]
    return []


# --- Validation Runner ---


def run_for_table(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    table_name: str,
    validations: List[Callable[[pd.DataFrame, str], List[ValidationRecord]]],
    report: List[ValidationRecord],
) -> None:
    try:
        logger.info(f"Validating table: {table_name}")
        tables = conn.execute("SHOW TABLES").fetchall()
        if table_name not in [t[0] for t in tables]:
            logger.warning(f"Table `{table_name}` not found in the database.")
            return
        df = conn.execute(query).fetch_df()
        logger.info(f"Loaded `{table_name}` — {len(df)} rows")
        for validate_func in validations:
            result = validate_func(df, table_name)
            if result:
                logger.info(f"Issues found in {table_name}: {len(result)}")
                report.extend(result)
    except Exception as e:
        logger.error(f"Error validating {table_name}: {e}")


def run_validations() -> None:
    """Run essential data validation checks on DuckDB tables."""
    logger.info("Validation script started.")
    conn = duckdb.connect(str(Config.DUCKDB_FILE))
    report: List[ValidationRecord] = []

    run_for_table(
        conn,
        "SELECT * FROM stock_prices",
        "stock_prices",
        [
            lambda df, name: validate_no_nulls(df, name, ["close", "market_cap"]),
            lambda df, name: validate_positive_values(
                df, name, ["close", "market_cap"]
            ),
            validate_price_spikes,
        ],
        report,
    )

    run_for_table(
        conn,
        "SELECT * FROM market_index",
        "market_index",
        [
            lambda df, name: validate_no_nulls(df, name, ["spy_close"]),
            lambda df, name: validate_positive_values(df, name, ["spy_close"]),
        ],
        report,
    )

    run_for_table(
        conn,
        "SELECT * FROM index_values",
        "index_values",
        [
            lambda df, name: validate_no_nulls(df, name, ["index_value", "spy_value"]),
            lambda df, name: validate_positive_values(
                df, name, ["index_value", "spy_value"]
            ),
        ],
        report,
    )

    run_for_table(
        conn,
        "SELECT * FROM index_metrics",
        "index_metrics",
        [
            lambda df, name: validate_no_nulls(df, name, ["index_value", "spy_close"]),
            lambda df, name: validate_positive_values(
                df, name, ["index_value", "spy_close"]
            ),
        ],
        report,
    )

    if report:
        df_report = pd.DataFrame(
            report, columns=["table", "issue", "column", "count", "details_file"]
        )
        df_report.to_csv(Config.VALIDATION_LOG, index=False)
        logger.warning(
            f"Validation issues found. Report saved to: {Config.VALIDATION_LOG}"
        )
    else:
        logger.info("All data passed validation checks. No issues found.")

    conn.close()
