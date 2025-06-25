"""
Module: data_ingestion

Description:
------------
This module ingests US stock data and SPY index prices into a DuckDB database
for a single specified date.

Design:
-------
- Parallel data fetching using ThreadPoolExecutor.
- Resilient HTTP session with retry logic.
- Per-ticker transaction isolation and type-safe casting.
- Handles missing or failed tickers gracefully.
- Logs failures and saves failed tickers to CSV.

Dependencies:
-------------
- duckdb
- pandas
- yfinance
- requests
- bs4
- src.config.Config
- src.logger.setup_logging

Tables:
-------
- stock_prices (date, ticker, close, market_cap)
- market_index (date, spy_close)
"""

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from pathlib import Path

import duckdb
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

from src.config import Config
from src.logger import setup_logging

logger = setup_logging(Config.INGESTION_LOG_FILE, logger_name="eqx.ingestion")


def create_requests_session() -> requests.Session:
    """Create a retry-enabled HTTP session for robust API communication."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


session = create_requests_session()


def get_finnhub_tickers() -> List[str]:
    """Fetch active US common stock tickers from Finnhub."""
    if not Config.FINNHUB_API_KEY:
        logger.error("Finnhub API key missing! Set FINNHUB_API_KEY.")
        return []

    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={Config.FINNHUB_API_KEY}"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        tickers = [
            item["symbol"]
            for item in data
            if item.get("type") == "Common Stock"
            and item.get("isEnabled", False)
            and item.get("status", "active").lower() == "active"
        ]
        logger.info(f"Fetched {len(tickers)} live tickers from Finnhub.")
        return tickers
    except Exception as e:
        logger.error(f"Failed to fetch tickers from Finnhub: {e}")
        return []


def get_sp500_tickers() -> List[str]:
    """Fallback method to fetch S&P 500 tickers from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        html = session.get(url, timeout=10).text
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "constituents"})
        tickers = [
            row.find_all("td")[0].text.strip().replace(".", "-")
            for row in table.find_all("tr")[1:]
        ]
        logger.info(f"Fetched {len(tickers)} tickers from Wikipedia S&P 500 list.")
        return tickers
    except Exception as e:
        logger.error(f"Failed to fetch S&P 500 tickers: {e}")
        return []


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create DuckDB tables if they do not exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            date DATE,
            ticker TEXT,
            close DOUBLE,
            market_cap DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_index (
            date DATE,
            spy_close DOUBLE
        )
        """
    )
    logger.info("Tables created.")


def fetch_and_prepare_stock_data(ticker: str, date: str) -> Optional[pd.DataFrame]:
    """Fetch and prepare stock price and market cap data for a given ticker and date."""
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        next_day = (dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        stock = yf.Ticker(ticker)
        df = stock.history(start=date, end=next_day)

        if df.empty:
            raise ValueError("No data found")

        shares_out = stock.info.get("sharesOutstanding")
        if not shares_out or shares_out <= 0:
            raise ValueError("Invalid sharesOutstanding")

        df = df.reset_index()[["Date", "Close"]]
        df["ticker"] = ticker
        df["market_cap"] = df["Close"] * shares_out
        df.rename(columns={"Date": "date", "Close": "close"}, inplace=True)

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

        df = df[df["date"] == dt.date()]
        return df.dropna()
    except Exception as e:
        logger.warning(f"[{ticker}] Fetch error: {e}")
        return None


def fetch_all_stocks_parallel(
    tickers: List[str],
    conn: duckdb.DuckDBPyConnection,
    date: str,
    max_workers: int = 8,
) -> None:
    """Fetch and insert stock data in parallel into DuckDB."""
    success_rows = 0
    failed_tickers: List[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_prepare_stock_data, t, date): t for t in tickers
        }

        for future in as_completed(futures):
            ticker = futures[future]
            try:
                df = future.result()
            except Exception as e:
                logger.warning(f"[{ticker}] Future failed: {e}")
                failed_tickers.append(ticker)
                continue

            if df is not None and not df.empty:
                try:
                    conn.execute("BEGIN")
                    conn.register("temp_df", df)
                    conn.execute(
                        """
                        INSERT INTO stock_prices
                        SELECT 
                            CAST(date AS DATE),
                            CAST(ticker AS TEXT),
                            CAST(close AS DOUBLE),
                            CAST(market_cap AS DOUBLE)
                        FROM temp_df
                        """
                    )
                    conn.unregister("temp_df")
                    conn.execute("COMMIT")
                    success_rows += len(df)
                    logger.info(f"[{ticker}] Inserted {len(df)} rows.")
                except Exception as insert_err:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception as rollback_err:
                        logger.error(f"[{ticker}] Rollback also failed: {rollback_err}")
                    logger.warning(f"[{ticker}] Insertion failed: {insert_err}")
                    failed_tickers.append(ticker)
            else:
                failed_tickers.append(ticker)

    if failed_tickers:
        pd.DataFrame({"failed_ticker": failed_tickers}).to_csv(
            Config.FAILED_TICKERS_FILE, index=False
        )
        logger.warning(f"Failed tickers saved to {Config.FAILED_TICKERS_FILE}")

    logger.info(f"Total rows inserted: {success_rows}")


def fetch_spy_data(conn: duckdb.DuckDBPyConnection, date: str) -> None:
    """Fetch and insert SPY (S&P 500 Index) closing price for the given date."""
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        next_day = (dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        spy = yf.Ticker("^GSPC")
        df = spy.history(start=date, end=next_day)

        if df.empty:
            raise ValueError("SPY data is empty")

        df = df.reset_index()[["Date", "Close"]]
        df.rename(columns={"Date": "date", "Close": "spy_close"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["spy_close"] = pd.to_numeric(df["spy_close"], errors="coerce")
        df.dropna(subset=["date", "spy_close"], inplace=True)
        df = df[df["date"] == dt.date()]

        conn.execute("BEGIN TRANSACTION")
        conn.register("temp_index_df", df)
        conn.execute("INSERT INTO market_index SELECT * FROM temp_index_df")
        conn.unregister("temp_index_df")
        conn.execute("COMMIT")

        logger.info("SPY index data inserted successfully.")
    except Exception as e:
        logger.warning(f"Failed to fetch SPY data: {e}")


def run_ingestion(date: Optional[str] = None) -> None:
    """
    Ingests stock price data and SPY index value into DuckDB for a specific date.

    Args:
        date (Optional[str]): Target date in 'YYYY-MM-DD' format.
    """
    if not Config.FINNHUB_API_KEY:
        logger.error("Cannot proceed without FINNHUB_API_KEY.")
        return

    if not date:
        logger.error("Must provide `date`. Aborting ingestion.")
        return

    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        logger.error("Invalid date format. Use YYYY-MM-DD.")
        return

    logger.info(f"Ingesting data for: {date}")

    db_path = Path(Config.DUCKDB_FILE)

    try:
        conn = duckdb.connect(str(db_path))
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        return

    try:
        logger.info("Ensuring DuckDB schema is ready.")
        create_tables(conn)

        tickers = get_finnhub_tickers()
        if not tickers:
            logger.warning("Finnhub failed. Falling back to S&P 500 tickers.")
            tickers = get_sp500_tickers()

        if not tickers:
            logger.error("No tickers found. Aborting ingestion.")
            return

        fetch_all_stocks_parallel(tickers, conn, date)
        fetch_spy_data(conn, date)

    finally:
        conn.close()
