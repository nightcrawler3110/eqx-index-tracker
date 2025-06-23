import duckdb
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pathlib import Path

# --- Config and Setup ---
BASE_DIR = Path(__file__).resolve().parent.parent  # Project root
DB_FILE = BASE_DIR / "eqx_index.db"
REPORTS_DIR = BASE_DIR / "reports"
DETAILED_ISSUES_DIR = REPORTS_DIR / "detailed_issues"
LOGS_DIR = BASE_DIR / "logs"
VALIDATION_LOG = REPORTS_DIR / "data_validation_report.csv"

# Ensure folders exist BEFORE logging
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DETAILED_ISSUES_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "data_validation.log"),
        logging.StreamHandler()
    ],
    force=True
)
logging.info("Validation script started.")

# --- Validation Functions ---

def validate_no_nulls(df, table_name):
    nulls = df.isnull().sum()
    issues = nulls[nulls > 0]
    records = []
    for col, count in issues.items():
        bad_rows = df[df[col].isnull()]
        path = DETAILED_ISSUES_DIR / f"{table_name}__nulls__{col}.csv"
        bad_rows.to_csv(path, index=False)
        records.append((table_name, 'Null values', col, int(count), str(path)))
    return records

def validate_positive_values(df, table_name, columns):
    records = []
    for col in columns:
        bad_rows = df[df[col] <= 0]
        if not bad_rows.empty:
            path = DETAILED_ISSUES_DIR / f"{table_name}__non_positive__{col}.csv"
            bad_rows.to_csv(path, index=False)
            records.append((table_name, 'Non-positive values', col, len(bad_rows), str(path)))
    return records

def validate_duplicates(df, table_name):
    try:
        hashable_cols = [col for col in df.columns if not df[col].apply(lambda x: isinstance(x, (list, np.ndarray))).any()]
        bad_rows = df[df.duplicated(subset=hashable_cols, keep=False)]
        if not bad_rows.empty:
            path = DETAILED_ISSUES_DIR / f"{table_name}__duplicate_rows.csv"
            bad_rows.to_csv(path, index=False)
            return [(table_name, 'Duplicate rows', 'hashable subset', len(bad_rows), str(path))]
    except Exception as e:
        logging.warning(f"[{table_name}] Duplicate check skipped: {e}")
    return []

def validate_date_monotonic(df, table_name, date_col):
    sorted_df = df.sort_values(date_col)
    if not sorted_df[date_col].is_monotonic_increasing:
        path = DETAILED_ISSUES_DIR / f"{table_name}__non_monotonic__{date_col}.csv"
        sorted_df.to_csv(path, index=False)
        return [(table_name, 'Non-monotonic dates', date_col, 1, str(path))]
    return []

def validate_index_range(df, table_name, col):
    if df[col].min() < 0 or df[col].max() > 100000:
        path = DETAILED_ISSUES_DIR / f"{table_name}__index_range__{col}.csv"
        df.to_csv(path, index=False)
        return [(table_name, 'Abnormal range', col, 1, str(path))]
    return []

def validate_price_spikes(df, table_name):
    if 'ticker' not in df.columns or 'close' not in df.columns or 'date' not in df.columns:
        return []
    
    df = df.sort_values(['ticker', 'date'])
    df['prev_close'] = df.groupby('ticker')['close'].shift(1)
    df['change_pct'] = (df['close'] - df['prev_close']) / df['prev_close']
    
    # Check for spike > 10x (1000% change)
    bad_rows = df[df['change_pct'].abs() > 10]
    
    if not bad_rows.empty:
        path = DETAILED_ISSUES_DIR / f"{table_name}__price_spike_gt_10x.csv"
        bad_rows.to_csv(path, index=False)
        return [(table_name, 'Price change >10x vs previous day', 'close', len(bad_rows), str(path))]
    
    return []


# --- Run Validations ---

def run_validations():
    conn = duckdb.connect(str(DB_FILE))
    report = []

    def run_for_table(query, table_name, validations):
        try:
            logging.info(f"Validating table: {table_name}")
            tables = conn.execute("SHOW TABLES").fetchall()
            logging.info(f"Available tables in DB: {[t[0] for t in tables]}")
            if table_name not in [t[0] for t in tables]:
                logging.warning(f"Table `{table_name}` not found in the database.")
                return

            df = conn.execute(query).fetch_df()
            logging.info(f"Table `{table_name}` loaded — {len(df)} rows")

            for validate_func in validations:
                result = validate_func(df, table_name)
                if result:
                    logging.info(f"Issues found in {table_name}: {len(result)} validation records.")
                report.extend(result)
        except Exception as e:
            logging.error(f"Error validating {table_name}: {e}")

    run_for_table(
        "SELECT * FROM stock_prices",
        "stock_prices",
        [
            validate_no_nulls,
            lambda df, name: validate_positive_values(df, name, ['close', 'market_cap']),
            validate_duplicates,
            lambda df, name: validate_date_monotonic(df, name, 'date'),
            validate_price_spikes
        ]
    )

    run_for_table(
        "SELECT * FROM market_index",
        "market_index",
        [
            validate_no_nulls,
            lambda df, name: validate_positive_values(df, name, ['spy_close']),
            validate_duplicates,
            lambda df, name: validate_date_monotonic(df, name, 'date')
        ]
    )

    run_for_table(
        "SELECT * FROM index_values",
        "index_values",
        [
            validate_no_nulls,
            lambda df, name: validate_positive_values(df, name, ['index_value', 'spy_value']),
            validate_duplicates,
            lambda df, name: validate_date_monotonic(df, name, 'date'),
            lambda df, name: validate_index_range(df, name, 'index_value')
        ]
    )

    run_for_table(
        "SELECT * FROM index_metrics",
        "index_metrics",
        [
            validate_no_nulls,
            lambda df, name: validate_positive_values(df, name, ['index_value', 'spy_close']),
            validate_duplicates,
            lambda df, name: validate_date_monotonic(df, name, 'date')
        ]
    )

    run_for_table(
        "SELECT * FROM summary_metrics",
        "summary_metrics",
        [validate_no_nulls]
    )

    if report:
        df_report = pd.DataFrame(report, columns=["table", "issue", "column", "count", "details_file"])
        df_report.to_csv(VALIDATION_LOG, index=False)
        logging.warning(f"Validation issues found. Report saved to: {VALIDATION_LOG}")
    else:
        logging.info("All data passed validation checks. No issues found.")

    conn.close()

# --- Entrypoint ---
if __name__ == "__main__":
    run_validations()
