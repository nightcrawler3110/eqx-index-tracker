import os
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yfinance as yf
import duckdb
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup

# --- Configuration and constants ---
BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

class Config:
    DUCKDB_FILE = os.getenv("DUCKDB_FILE", str(BASE_DIR / "eqx_index.db"))
    LOG_FILE = str(LOGS_DIR / "ingestion.log")
    FAILED_TICKERS_FILE = str(BASE_DIR / "failed_tickers.csv")
    FETCH_DAYS = int(os.getenv("FETCH_DAYS", "30"))
    FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# --- Logging setup ---
def setup_logging():
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(Config.LOG_FILE),
                logging.StreamHandler()
            ]
        )

# --- HTTP session with retry ---
def create_requests_session():
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
def get_finnhub_tickers():
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
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        html = session.get(url, timeout=10).text
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table", {"id": "constituents"})
        tickers = []

        for row in table.find_all("tr")[1:]:
            ticker = row.find_all("td")[0].text.strip()
            tickers.append(ticker.replace(".", "-"))

        logging.info(f"Fetched {len(tickers)} tickers from Wikipedia S&P 500 list.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to fetch S&P 500 tickers: {e}")
        return []

# --- Schema creation ---
def create_tables(conn):
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
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_prices ON stock_prices (date, ticker)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_market_index ON market_index (date)")
    logging.info("Tables and indexes created successfully.")

# --- Stock data fetch ---
def fetch_and_prepare_stock_data(ticker, start_date, end_date):
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
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['market_cap'] = pd.to_numeric(df['market_cap'], errors='coerce')

        return df if not df.empty else None
    except Exception as e:
        logging.warning(f"[{ticker}] Fetch error: {e}")
        return None

# --- Parallel ingestion with safe upsert ---
def fetch_all_stocks_parallel(tickers, conn, start_date, end_date, max_workers=8):
    success_rows = 0
    failed_tickers = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_prepare_stock_data, t, start_date, end_date): t for t in tickers}

        conn.execute("BEGIN TRANSACTION")
        try:
            for future in as_completed(futures):
                ticker = futures[future]
                df = future.result()

                if df is not None and set(df.columns) >= {"date", "ticker", "close", "market_cap"}:
                    try:
                        conn.register("temp_df", df)

                        # DELETE via JOIN (DuckDB-safe)
                        deleted = conn.execute("""
                            DELETE FROM stock_prices
                            USING temp_df
                            WHERE stock_prices.date = temp_df.date AND stock_prices.ticker = temp_df.ticker
                        """).fetchall()
                        
                        conn.execute("""
                            INSERT INTO stock_prices (date, ticker, close, market_cap)
                            SELECT date, ticker, close, market_cap FROM temp_df
                        """)

                        conn.unregister("temp_df")
                        success_rows += len(df)
                        logging.info(f"[{ticker}] Upserted {len(df)} rows.")
                    except Exception as insert_err:
                        logging.warning(f"[{ticker}] Insertion failed: {insert_err}")
                        failed_tickers.append(ticker)
                else:
                    failed_tickers.append(ticker)

            conn.execute("COMMIT")
        except Exception as e:
            conn.execute("ROLLBACK")
            logging.error(f"Transaction failed: {e}")
            failed_tickers.extend([futures[f] for f in futures if futures[f] not in failed_tickers])

    if failed_tickers:
        pd.DataFrame({'failed_ticker': failed_tickers}).to_csv(Config.FAILED_TICKERS_FILE, index=False)
        logging.warning(f"Failed tickers saved to {Config.FAILED_TICKERS_FILE}")

    logging.info(f"Total rows upserted: {success_rows}")

# --- SPY index data fetch ---
def fetch_spy_data(conn, start_date, end_date):
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
        conn.execute("""
            DELETE FROM market_index
            USING temp_index_df
            WHERE market_index.date = temp_index_df.date
        """)
        conn.execute("INSERT INTO market_index SELECT * FROM temp_index_df")
        conn.unregister("temp_index_df")
        conn.execute("COMMIT")

        logging.info("SPY index data inserted successfully.")
    except Exception as e:
        logging.warning(f"Failed to fetch SPY data: {e}")

# --- Main Ingestion Entry Point ---
def run_ingestion():
    setup_logging()
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


        #     ┌────────────────────────┐
        #     │ 1. Setup & Config      │
        #     │ - Paths & ENV config   │
        #     └─────────┬──────────────┘
        #               │
        #               ▼
        #     ┌────────────────────────┐
        #     │ 2. Initialize Logging  │
        #     └─────────┬──────────────┘
        #               │
        #               ▼
        #     ┌────────────────────────────┐
        #     │ 3. Get Tickers             │
        #     │ - Try Finnhub API          │
        #     │ - Fallback: Wikipedia S&P500│
        #     └─────────┬──────────────────┘
        #               │
        #               ▼
        #     ┌────────────────────────────┐
        #     │ 4. Connect to DuckDB       │
        #     │ - Create Tables if Needed  │
        #     └─────────┬──────────────────┘
        #               │
        #               ▼
        # ┌─────────────────────────────────────┐
        # │ 5. Fetch Stock Data (Parallel)       │
        # │ - Use yfinance for historical prices │
        # │ - Calculate market cap               │
        # │ - Validate + Upsert into DuckDB      │
        # └────────────────┬────────────────────┘
        #                  │
        #                  ▼
        # ┌──────────────────────────────────────┐
        # │ 6. Fetch SPY Index (^GSPC)            │
        # │ - Clean and Upsert to market_index    │
        # └────────────────┬─────────────────────┘
        #                  │
        #                  ▼
        #       ┌────────────────────────┐
        #       │ 7. Done (Log Summary)  │
        #       └────────────────────────┘
