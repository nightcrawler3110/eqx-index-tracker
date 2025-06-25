import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import pandas as pd
import duckdb
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

from src.config import Config
from src.logger import setup_logging
from src.data_ingestion import (
    get_finnhub_tickers,
    get_sp500_tickers,
    create_tables,
    fetch_spy_data,
)
from src.index_builder import build_index
from src.daily_metrics_calculator import compute_daily_metrics

logger = setup_logging(Config.INGESTION_LOG_FILE, logger_name="eqx.historical_pipeline")


def fetch_ticker_range(
    ticker: str, start_date: str, end_date: str
) -> Optional[pd.DataFrame]:
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(
            start=start_date,
            end=(datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)),
        )

        if df.empty:
            return None

        shares_out = stock.info.get("sharesOutstanding")
        if not shares_out or shares_out <= 0:
            return None

        df = df.reset_index()[["Date", "Close"]]
        df["ticker"] = ticker
        df["market_cap"] = df["Close"] * shares_out
        df.rename(columns={"Date": "date", "Close": "close"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df.dropna()
        return df
    except Exception as e:
        logger.warning(f"[{ticker}] Error fetching data: {e}")
        return None


def ingest_all_data(start_date: str, end_date: str, max_workers: int = 12) -> None:
    logger.info(f"Starting full ingestion from {start_date} to {end_date}")

    tickers = get_finnhub_tickers() or get_sp500_tickers()
    if not tickers:
        logger.error("No tickers available to ingest.")
        return

    db_path = Path(Config.DUCKDB_FILE)
    conn = duckdb.connect(str(db_path))
    create_tables(conn)

    success_rows = 0
    failed_tickers = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_ticker_range, t, start_date, end_date): t
            for t in tickers
        }

        for future in as_completed(futures):
            ticker = futures[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    conn.execute("BEGIN")
                    conn.register("temp_df", df)
                    conn.execute(
                        """
                        INSERT INTO stock_prices
                        SELECT CAST(date AS DATE), CAST(ticker AS TEXT), 
                               CAST(close AS DOUBLE), CAST(market_cap AS DOUBLE)
                        FROM temp_df
                    """
                    )
                    conn.unregister("temp_df")
                    conn.execute("COMMIT")
                    logger.info(f"[{ticker}] Inserted {len(df)} rows.")
                    success_rows += len(df)
                else:
                    failed_tickers.append(ticker)
            except Exception as e:
                conn.execute("ROLLBACK")
                logger.warning(f"[{ticker}] Failed to ingest: {e}")
                failed_tickers.append(ticker)

    # Ingest SPY
    for date in pd.date_range(start=start_date, end=end_date):
        fetch_spy_data(conn, date.strftime("%Y-%m-%d"))

    if failed_tickers:
        pd.DataFrame({"failed_ticker": failed_tickers}).to_csv(
            Config.FAILED_TICKERS_FILE, index=False
        )
        logger.warning(f"Saved failed tickers to: {Config.FAILED_TICKERS_FILE}")

    logger.info(f"Ingestion complete: {success_rows} rows inserted.")
    conn.close()


def run_pipeline(start_date: str, end_date: str) -> None:
    ingest_all_data(start_date, end_date)

    date_range = pd.date_range(start=start_date, end=end_date)
    for d in date_range:
        date_str = d.strftime("%Y-%m-%d")
        build_index(date_str)
        compute_daily_metrics(date_str)

    logger.info("Full pipeline completed for all dates.")


def parse_args():
    parser = argparse.ArgumentParser(description="Run historical EQX pipeline")
    parser.add_argument(
        "--end_date", required=True, help="End date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--days", type=int, default=30, help="Number of days to go back from end_date"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        start_dt = end_dt - timedelta(days=args.days - 1)
        run_pipeline(
            start_date=start_dt.strftime("%Y-%m-%d"),
            end_date=end_dt.strftime("%Y-%m-%d"),
        )
    except ValueError as ve:
        logger.error(f"Invalid date format: {ve}")
