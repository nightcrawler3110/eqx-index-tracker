"""
Module: index_builder

Description:
------------
This module constructs a custom equal-weighted index of the top 100 US stocks by market capitalization
for each available date using data from a DuckDB database.

Key Features:
-------------
- Computes the index as the average close price of the top 100 stocks (equal weights).
- Fetches SPY close values for benchmark comparison.
- Saves daily index values with tickers used per day into the `index_values` table.
- Designed for append-only behavior (no deletes, no uniqueness constraint).

Functions:
----------
- fetch_top_100_by_market_cap(): Get top 100 tickers by market cap on a given date.
- fetch_spy_value(): Retrieve SPY index value on a given date.
- build_index(): Main entry point to compute and store index data.

Dependencies:
-------------
- pandas
- duckdb
- src.config.Config
- src.logger.setup_logging

Logging:
--------
Logs steps and errors to the configured INDEX_BUILDER_LOG_FILE.
"""

import duckdb
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

from src.config import Config
from src.logger import setup_logging

# --- Initialize Logging ---
setup_logging(Config.INDEX_BUILDER_LOG_FILE)


def fetch_top_100_by_market_cap(conn: duckdb.DuckDBPyConnection, date: str) -> Optional[pd.DataFrame]:
    """Fetch top 100 tickers by market cap on a given date."""
    try:
        df = conn.execute(f"""
            SELECT ticker, close
            FROM stock_prices
            WHERE date = '{date}'
            ORDER BY market_cap DESC
            LIMIT 100
        """).fetch_df()

        if len(df) < 100:
            logging.warning(f"[{date}] Less than 100 stocks available. Skipping.")
            return None
        return df
    except Exception as e:
        logging.error(f"[{date}] Failed to fetch top 100: {e}")
        return None


def fetch_spy_value(conn: duckdb.DuckDBPyConnection, date: str) -> Optional[float]:
    """Fetch SPY close value on a given date."""
    try:
        df = conn.execute(f"SELECT spy_close FROM market_index WHERE date = '{date}'").fetch_df()
        return round(df['spy_close'].iloc[0], 4) if not df.empty else None
    except Exception as e:
        logging.warning(f"[{date}] Failed to fetch SPY value: {e}")
        return None


def build_index() -> None:
    """Builds and appends index values to DuckDB based on top 100 market cap stocks per day."""
    logging.info("Starting index build process.")

    if not Path(Config.DUCKDB_FILE).exists():
        logging.error(f"DuckDB file not found: {Config.DUCKDB_FILE}")
        return

    conn = duckdb.connect(Config.DUCKDB_FILE)
    try:
        stock_df = conn.execute("SELECT * FROM stock_prices").fetch_df()
        if stock_df.empty:
            logging.warning("No data in `stock_prices`. Aborting index build.")
            return
        logging.info(f"Loaded {len(stock_df)} rows from stock_prices.")

        dates_df = conn.execute("SELECT DISTINCT date FROM stock_prices ORDER BY date").fetch_df()
        logging.info(f"{len(dates_df)} distinct dates found.")

        index_data = []

        for date in dates_df['date']:
            top_df = fetch_top_100_by_market_cap(conn, date)
            if top_df is None:
                continue

            index_val = round((top_df['close'] * (1 / 100)).sum(), 4)
            spy_val = fetch_spy_value(conn, date)

            index_data.append({
                "date": date,
                "index_value": index_val,
                "spy_value": spy_val,
                "tickers": ",".join(top_df['ticker'].tolist())
            })

        df_index = pd.DataFrame(index_data)
        if df_index.empty:
            logging.error("No index values calculated. Aborting.")
            return

        conn.execute("BEGIN TRANSACTION")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_values (
                date DATE,
                index_value DOUBLE,
                spy_value DOUBLE,
                tickers TEXT
            )
        """)

        conn.register("df_index", df_index)
        conn.execute("INSERT INTO index_values SELECT * FROM df_index")
        conn.unregister("df_index")
        conn.execute("COMMIT")

        logging.info(f"index_values appended with {len(df_index)} new rows.")
    except Exception as e:
        conn.execute("ROLLBACK")
        logging.error(f"Index build failed: {e}")
    finally:
        conn.close()
        logging.info("Index build process completed.")
