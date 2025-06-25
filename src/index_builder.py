"""
Module: index_builder

Description:
------------
Builds a custom equal-weighted index from the top 100 US stocks by market capitalization
and appends the index value to a DuckDB table for the given date.

Features:
---------
- Fetches top 100 stocks by market cap for a specific date.
- Computes equal-weighted index value.
- Includes SPY index value for benchmark comparison.
- Logs index construction status.
- Supports fault-tolerant index creation with proper rollback.

Tables:
-------
- index_values (date, index_value, spy_value, tickers)

Dependencies:
-------------
- duckdb
- pandas
- src.config.Config
- src.logger.setup_logging
"""

import duckdb
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

from src.config import Config
from src.logger import setup_logging

# --- Initialize logger ---
logger = setup_logging(Config.INDEX_BUILDER_LOG_FILE, logger_name="eqx.index_builder")


def fetch_top_100_by_market_cap(
    conn: duckdb.DuckDBPyConnection, date: str
) -> Optional[pd.DataFrame]:
    """
    Fetch the top 100 stocks by market capitalization on a given date.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to DuckDB.
        date (str): Target date in 'YYYY-MM-DD' format.

    Returns:
        Optional[pd.DataFrame]: DataFrame with tickers and close prices, or None on failure.
    """
    try:
        df = conn.execute(
            f"""
            SELECT ticker, close
            FROM stock_prices
            WHERE date = '{date}'
            ORDER BY market_cap DESC
            LIMIT 100
        """
        ).fetch_df()

        if len(df) < 100:
            logger.warning(f"[{date}] Less than 100 stocks available. Skipping.")
            return None
        return df
    except Exception as e:
        logger.error(f"[{date}] Failed to fetch top 100: {e}")
        return None


def fetch_spy_value(conn: duckdb.DuckDBPyConnection, date: str) -> Optional[float]:
    """
    Fetch the SPY (S&P 500) closing value for a specific date.

    Args:
        conn (duckdb.DuckDBPyConnection): Connection to DuckDB.
        date (str): Target date in 'YYYY-MM-DD' format.

    Returns:
        Optional[float]: Rounded SPY close value or None if not found.
    """
    try:
        df = conn.execute(
            f"SELECT spy_close FROM market_index WHERE date = '{date}'"
        ).fetch_df()
        return round(df["spy_close"].iloc[0], 4) if not df.empty else None
    except Exception as e:
        logger.warning(f"[{date}] Failed to fetch SPY value: {e}")
        return None


def build_index(date: str) -> None:
    """
    Compute and append the equal-weighted index value to DuckDB for a given date.

    Steps:
        - Validates DuckDB presence.
        - Fetches top 100 tickers by market cap.
        - Computes equal-weighted index from close prices.
        - Fetches SPY value for benchmarking.
        - Appends index data to `index_values` table.

    Args:
        date (str): Target date in 'YYYY-MM-DD' format.
    """
    logger.info(f"Starting index build for date: {date}")

    if not Path(Config.DUCKDB_FILE).exists():
        logger.error(f"DuckDB file not found: {Config.DUCKDB_FILE}")
        return

    conn = duckdb.connect(Config.DUCKDB_FILE)
    try:
        top_df = fetch_top_100_by_market_cap(conn, date)
        if top_df is None:
            logger.warning(f"No index calculated for {date}.")
            return

        index_val = round((top_df["close"] * (1 / 100)).sum(), 4)
        spy_val = fetch_spy_value(conn, date)

        df_index = pd.DataFrame(
            [
                {
                    "date": date,
                    "index_value": index_val,
                    "spy_value": spy_val,
                    "tickers": ",".join(top_df["ticker"].tolist()),
                }
            ]
        )

        conn.execute("BEGIN TRANSACTION")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS index_values (
                date DATE,
                index_value DOUBLE,
                spy_value DOUBLE,
                tickers TEXT
            )
        """
        )
        conn.register("df_index", df_index)
        conn.execute("INSERT INTO index_values SELECT * FROM df_index")
        conn.unregister("df_index")
        conn.execute("COMMIT")

        logger.info(f"index_values updated with data for {date}.")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error(f"Index build failed for {date}: {e}")
    finally:
        conn.close()
        logger.info("Index build process completed.")
