import duckdb
import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path

# --- Paths & Setup ---
BASE_DIR = Path(__file__).resolve().parent.parent  # ensure root-level paths
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

DUCKDB_FILE = BASE_DIR / "eqx_index.db"
OUTPUT_FILE = DATA_DIR / "equal_weight_index.xlsx"
LOG_FILE = LOGS_DIR / "export_excel.log"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ],
    force=True
)

def safe_split(x):
    try:
        if isinstance(x, (list, np.ndarray)):
            return list(x)
        elif isinstance(x, str) and x.startswith('[') and x.endswith(']'):
            return x.strip("[]").replace("'", "").split(',')
        elif isinstance(x, str):
            return x.split(",")
        else:
            return []
    except Exception as e:
        logging.warning(f"safe_split failed for input: {x} | Error: {e}")
        return []

def export_to_excel():
    logging.info("Starting export to Excel.")

    if not DUCKDB_FILE.exists():
        logging.error(f"DuckDB file not found at {DUCKDB_FILE}")
        return

    # Remove old file if corrupted/stale
    if OUTPUT_FILE.exists():
        try:
            os.remove(OUTPUT_FILE)
            logging.info(f"Deleted existing Excel file: {OUTPUT_FILE}")
        except Exception as e:
            logging.warning(f"Failed to delete old Excel file: {e}")

    # Connect and load data
    try:
        conn = duckdb.connect(str(DUCKDB_FILE))
        performance_df = conn.execute("""
            SELECT date, index_value, daily_return, cumulative_return
            FROM index_metrics
            ORDER BY date
        """).fetch_df()

        composition_df = conn.execute("""
            SELECT date, tickers FROM index_metrics ORDER BY date
        """).fetch_df()

        summary_df = conn.execute("SELECT * FROM summary_metrics").fetch_df()
        conn.close()
        logging.info("Loaded data from DuckDB successfully.")
    except Exception as e:
        logging.error(f"Error loading data from DuckDB: {e}")
        return

    # --- Transformations ---
    try:
        # Sheet 1: Index Performance
        index_performance = performance_df.copy()

        # Sheet 2: Daily Composition
        daily_composition = composition_df.copy()
        daily_composition['tickers'] = daily_composition['tickers'].apply(safe_split)
        exploded = daily_composition['tickers'].apply(pd.Series)
        exploded.columns = [f'ticker_{i+1}' for i in range(exploded.shape[1])]
        daily_composition_final = pd.concat([daily_composition['date'], exploded], axis=1)

        # Sheet 3: Composition Changes
        changes = []
        prev_set = set()
        for _, row in composition_df.iterrows():
            tickers_list = safe_split(row['tickers'])
            current_set = set(tickers_list)

            added = current_set - prev_set
            removed = prev_set - current_set
            intersection = current_set & prev_set

            changes.append({
                'date': row['date'],
                'added': ','.join(sorted(added)),
                'removed': ','.join(sorted(removed)),
                'intersection_size': len(intersection)
            })
            prev_set = current_set
        composition_changes = pd.DataFrame(changes)

        logging.info("Transformations completed.")
    except Exception as e:
        logging.error(f"Data transformation failed: {e}")
        return

    # --- Export to Excel ---
    try:
        with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
            index_performance.to_excel(writer, sheet_name="index_performance", index=False)
            daily_composition_final.to_excel(writer, sheet_name="daily_composition", index=False)
            composition_changes.to_excel(writer, sheet_name="composition_changes", index=False)
            summary_df.to_excel(writer, sheet_name="summary_metrics", index=False)
        logging.info(f"Successfully exported Excel to {OUTPUT_FILE}")
    except Exception as e:
        logging.error(f"Failed to write Excel file: {e}")
