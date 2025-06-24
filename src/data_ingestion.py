"""
Module: data_ingestion

Description:
------------
This module ingests historical US stock data and SPY index prices into a DuckDB database.
It fetches live stock tickers from Finnhub (with fallback to the S&P 500 list from Wikipedia),
downloads historical price data via yfinance, calculates market caps using share counts,
and appends the results into normalized DuckDB tables.

Design:
-------
- Parallel data fetching using ThreadPoolExecutor.
- Resilient HTTP session with retry logic.
- Data integrity enforced via per-ticker transactions and explicit column type casting.
- Graceful handling of bad tickers, missing data, and API errors.
- Logging and CSV tracking of failed ticker ingestions.

Main Functions:
---------------
- create_requests_session(): Prepares an HTTP session with retry strategy.
- get_finnhub_tickers(): Fetches US stock tickers from Finnhub API.
- get_sp500_tickers(): Fallback function to retrieve S&P 500 tickers from Wikipedia.
- create_tables(conn): Creates DuckDB tables for stock prices and market index data.
- fetch_and_prepare_stock_data(ticker, start_date, end_date): Downloads and formats stock data.
- fetch_all_stocks_parallel(tickers, conn, start_date, end_date): Ingests all tickers in parallel.
- fetch_spy_data(conn, start_date, end_date): Appends SPY (S&P 500) index price data.
- run_ingestion(): Main orchestrator for running the entire ingestion pipeline.

Dependencies:
-------------
- duckdb
- pandas
- yfinance
- requests
- bs4 (BeautifulSoup)
- src.config.Config (project-specific configuration module)
- src.logger.setup_logging (project-specific logging setup)

Tables Created:
---------------
- stock_prices (
      date DATE,
      ticker TEXT,
      close DOUBLE,
      market_cap DOUBLE
  )

- market_index (
      date DATE,
      spy_close DOUBLE
  )

Notes:
------
- The pipeline appends new data and does not enforce deduplication or uniqueness.
- Stock prices are enriched with market cap using yfinance's `sharesOutstanding` data.
- Each ticker's data is inserted in its own transaction to isolate failures.
- Failed tickers are logged and saved to a CSV defined in `Config.FAILED_TICKERS_FILE`.
"""


import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

import duckdb
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

from src.config import Config
from src.logger import setup_logging

# --- Initialize Logging ---
setup_logging(Config.INGESTION_LOG_FILE)

# --- HTTP session with retry ---
def create_requests_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = create_requests_session()

# --- Fetch tickers from Finnhub ---
def get_finnhub_tickers() -> List[str]:
    if not Config.FINNHUB_API_KEY:
        logging.error("Finnhub API key missing! Set FINNHUB_API_KEY.")
        return []

    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={Config.FINNHUB_API_KEY}"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        tickers = [
            item['symbol'] for item in data
            if item.get('type') == 'Common Stock' and item.get('isEnabled', False)
            and item.get('status', 'active').lower() == 'active'
        ]
        logging.info(f"Fetched {len(tickers)} live tickers from Finnhub.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to fetch tickers from Finnhub: {e}")
        return []

# --- Wikipedia fallback for S&P 500 ---
def get_sp500_tickers() -> List[str]:
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        html = session.get(url, timeout=10).text
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "constituents"})
        tickers = [
            row.find_all("td")[0].text.strip().replace(".", "-")
            for row in table.find_all("tr")[1:]
        ]
        logging.info(f"Fetched {len(tickers)} tickers from Wikipedia S&P 500 list.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to fetch S&P 500 tickers: {e}")
        return []

# --- Schema creation ---
def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            date DATE,
            ticker TEXT,
            close DOUBLE,
            market_cap DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_index (
            date DATE,
            spy_close DOUBLE
        )
    """)
    logging.info("Tables created.")

# --- Stock data fetch ---
def fetch_and_prepare_stock_data(ticker: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(start=start_date, end=end_date)

        if df.empty:
            raise ValueError("No data found")

        shares_out = stock.info.get("sharesOutstanding")
        if not shares_out or shares_out <= 0:
            raise ValueError("Invalid sharesOutstanding")

        df = df.reset_index()[['Date', 'Close']]
        df['ticker'] = ticker
        df['market_cap'] = df['Close'] * shares_out
        df.rename(columns={"Date": "date", "Close": "close"}, inplace=True)

        df['date'] = pd.to_datetime(df['date'])
        # Strip timezone if present
        if pd.api.types.is_datetime64tz_dtype(df['date']):
            df['date'] = df['date'].dt.tz_localize(None)

        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')
        df = df.astype({
            "ticker": "string",
            "close": "float64",
            "market_cap": "float64",
            "date": "datetime64[ns]"
        })

        return df.dropna()
    except Exception as e:
        logging.warning(f"[{ticker}] Fetch error: {e}")
        return None

# --- Parallel ingestion with type-safe casting ---
def fetch_all_stocks_parallel(tickers: List[str], conn: duckdb.DuckDBPyConnection,
                               start_date: str, end_date: str, max_workers: int = 8) -> None:
    success_rows = 0
    failed_tickers: List[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_and_prepare_stock_data, t, start_date, end_date): t
            for t in tickers
        }

        for future in as_completed(futures):
            ticker = futures[future]
            try:
                df = future.result()
            except Exception as e:
                logging.warning(f"[{ticker}] Future failed: {e}")
                failed_tickers.append(ticker)
                continue

            if df is not None and set(df.columns) >= {"date", "ticker", "close", "market_cap"}:
                try:
                    conn.execute("BEGIN")
                    conn.register("temp_df", df)
                    conn.execute("""
                        INSERT INTO stock_prices
                        SELECT 
                            CAST(date AS DATE),
                            CAST(ticker AS TEXT),
                            CAST(close AS DOUBLE),
                            CAST(market_cap AS DOUBLE)
                        FROM temp_df
                    """)
                    conn.unregister("temp_df")
                    conn.execute("COMMIT")
                    success_rows += len(df)
                    logging.info(f"[{ticker}] Inserted {len(df)} rows.")
                except Exception as insert_err:
                    try:
                        conn.execute("ROLLBACK")
                    except Exception as rollback_err:
                        logging.error(f"[{ticker}] Rollback also failed: {rollback_err}")
                    logging.warning(f"[{ticker}] Insertion failed: {insert_err}")
                    failed_tickers.append(ticker)
            else:
                failed_tickers.append(ticker)

    if failed_tickers:
        pd.DataFrame({'failed_ticker': failed_tickers}).to_csv(Config.FAILED_TICKERS_FILE, index=False)
        logging.warning(f"Failed tickers saved to {Config.FAILED_TICKERS_FILE}")

    logging.info(f"Total rows inserted: {success_rows}")

# --- SPY index data fetch (append-only) ---
def fetch_spy_data(conn: duckdb.DuckDBPyConnection, start_date: str, end_date: str) -> None:
    try:
        spy = yf.Ticker("^GSPC")
        df = spy.history(start=start_date, end=end_date)

        if df.empty:
            raise ValueError("SPY data is empty")

        df = df.reset_index()[['Date', 'Close']]
        df.rename(columns={'Date': 'date', 'Close': 'spy_close'}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df['spy_close'] = pd.to_numeric(df['spy_close'], errors='coerce')
        df.dropna(subset=['date', 'spy_close'], inplace=True)
        df = df[df['spy_close'] > 0]

        conn.execute("BEGIN TRANSACTION")
        conn.register("temp_index_df", df)
        conn.execute("INSERT INTO market_index SELECT * FROM temp_index_df")
        conn.unregister("temp_index_df")
        conn.execute("COMMIT")

        logging.info("SPY index data inserted successfully.")
    except Exception as e:
        logging.warning(f"Failed to fetch SPY data: {e}")

# --- Main Entry Point ---
def run_ingestion() -> None:
    if not Config.FINNHUB_API_KEY:
        logging.error("Cannot proceed without FINNHUB_API_KEY.")
        return

    conn = duckdb.connect(Config.DUCKDB_FILE)
    try:
        create_tables(conn)

        tickers = get_finnhub_tickers()
        if not tickers:
            logging.warning("Finnhub failed. Falling back to S&P 500 tickers.")
            tickers = get_sp500_tickers()

        if not tickers:
            logging.error("No tickers found. Aborting ingestion.")
            return

        end_date = datetime.today().strftime('%Y-%m-%d')
        start_date = (datetime.today() - timedelta(days=Config.FETCH_DAYS)).strftime('%Y-%m-%d')

        fetch_all_stocks_parallel(tickers, conn, start_date, end_date)
        fetch_spy_data(conn, start_date, end_date)

        logging.info("Data ingestion complete.")
    finally:
        conn.close()
