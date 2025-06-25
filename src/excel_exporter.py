import os
import ast
import logging
from pathlib import Path
from typing import List, Any, Optional
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import numpy as np

from src.config import Config
from src.logger import setup_logging

logger = setup_logging(Config.EXCEL_EXPORT_LOG_FILE, logger_name="eqx.excel_exporter")


def safe_split(x: Any) -> List[str]:
    """Safely split tickers from stringified lists or comma-separated strings."""
    try:
        if isinstance(x, (list, np.ndarray)):
            return list(x)
        elif isinstance(x, str):
            x = x.strip()
            if x.startswith("[") and x.endswith("]"):
                try:
                    parsed = ast.literal_eval(x)
                    if isinstance(parsed, list):
                        return [str(item).strip() for item in parsed]
                except Exception:
                    pass
            return [
                item.strip()
                for item in x.strip("[]").replace("'", "").split(",")
                if item.strip()
            ]
        return []
    except Exception as e:
        logger.warning(f"safe_split failed for input: {x} | Error: {e}")
        return []


def load_data_from_duckdb(
    start_date: str, end_date: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch filtered data from DuckDB for export."""
    conn = duckdb.connect(Config.DUCKDB_FILE)
    try:
        performance_df = conn.execute(
            f"""
            SELECT date, index_value, daily_return, cumulative_return
            FROM index_metrics
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date
            """
        ).fetch_df()

        composition_df = conn.execute(
            f"""
            SELECT date, tickers
            FROM index_metrics
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date
            """
        ).fetch_df()

        summary_df = conn.execute(
            f"""
            SELECT *
            FROM summary_metrics
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY date
            """
        ).fetch_df()

        logger.info(f"Data loaded from DuckDB for window: {start_date} → {end_date}")
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

    for idx, row in composition_df.iterrows():
        current_set = set(safe_split(row["tickers"]))

        if idx == 0:
            changes.append(
                {
                    "date": row["date"],
                    "added": ",".join(sorted(current_set)),
                    "removed": "",
                    "intersection_size": 0,
                }
            )
        else:
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
    output_path: str,
) -> None:
    """Write multiple dataframes to a single Excel file."""
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        performance_df.to_excel(writer, sheet_name="index_performance", index=False)
        composition_final.to_excel(writer, sheet_name="daily_composition", index=False)
        changes_df.to_excel(writer, sheet_name="composition_changes", index=False)
        summary_df.to_excel(writer, sheet_name="summary_metrics", index=False)


def export_to_excel(date: str, output_dir: Optional[str] = None) -> None:
    """
    Main function to export index data to a dated Excel file.

    Args:
        date (str): End date in 'YYYY-MM-DD' format.
        output_dir (str, optional): Directory where the Excel file will be saved.
                                    Defaults to Config.EXCEL_OUTPUT_DIR.
    """
    logger.info("Starting Excel export process...")

    try:
        end_date_dt = datetime.strptime(date, "%Y-%m-%d").date()
        start_date_dt = end_date_dt - timedelta(days=Config.get_fetch_days())

        # Use provided directory or fallback to config
        output_dir_path = (
            Path(output_dir) if output_dir else Path(Config.EXCEL_OUTPUT_DIR)
        )
        output_dir_path.mkdir(parents=True, exist_ok=True)

        # Generate filename with date
        filename = f"eqx_index_export_{end_date_dt}.xlsx"
        output_path = output_dir_path / filename

        # Load and transform data
        performance_df, composition_df, summary_df = load_data_from_duckdb(
            str(start_date_dt), str(end_date_dt)
        )
        composition_final = transform_composition(composition_df)
        changes_df = compute_composition_changes(composition_df)

        # Write to Excel
        write_excel(
            performance_df, composition_final, changes_df, summary_df, str(output_path)
        )
        logger.info(f"Excel export successful: {output_path}")

    except Exception as e:
        logger.error(f"Export failed: {e}")
