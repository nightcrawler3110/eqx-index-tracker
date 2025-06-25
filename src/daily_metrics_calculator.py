"""
Module: daily_metrics_calculator

Description:
------------
Computes per-day index metrics such as daily return, SPY return, drawdown, volatility,
turnover, and exposure similarity for a single date. This module only calculates single-day
metrics and not multi-day summary statistics.

Inputs:
-------
- date (str): Target date in 'YYYY-MM-DD' format.

Outputs:
--------
- Inserts computed metrics for the target date into the `index_metrics` DuckDB table.

Tables Used:
------------
- index_values (read)
- market_index (read)
- index_metrics (write)

Dependencies:
-------------
- duckdb
- pandas
- numpy
- src.config.Config
- src.logger.setup_logging
"""

from pathlib import Path
import pandas as pd
import numpy as np
import duckdb
import logging
from datetime import datetime, timedelta

from src.config import Config
from src.logger import setup_logging

logger = setup_logging(Config.METRICS_LOG_FILE, logger_name="eqx.daily_metrics")


def compute_daily_metrics(date: str) -> None:
    """
    Compute and persist daily metrics for a custom index on the given date.

    Metrics Computed:
    -----------------
    - Index and SPY daily returns
    - Cumulative return
    - Rolling volatility and beta (7-day)
    - Drawdown and drawdown percentage
    - Exposure turnover and similarity

    Args:
        date (str): Date in 'YYYY-MM-DD' format.

    Returns:
        None
    """
    logger.info(f"Computing daily metrics for {date}")

    if not Config.DUCKDB_FILE or not Path(Config.DUCKDB_FILE).exists():
        logger.error(f"DuckDB file not found at {Config.DUCKDB_FILE}")
        return

    conn = duckdb.connect(str(Config.DUCKDB_FILE))

    try:
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        lookback_start = (date_obj - timedelta(days=6)).strftime("%Y-%m-%d")

        index_df = conn.execute(
            f"""
            SELECT * FROM index_values
            WHERE date BETWEEN '{lookback_start}' AND '{date}'
            ORDER BY date
            """
        ).fetch_df()

        spy_df = conn.execute(
            f"""
            SELECT * FROM market_index
            WHERE date BETWEEN '{lookback_start}' AND '{date}'
            ORDER BY date
            """
        ).fetch_df()

        spy_df.rename(columns={"spy_value": "spy_close"}, inplace=True)

        if index_df.empty or spy_df.empty:
            logger.warning(f"No index or SPY data found up to {date}")
            return

        df = index_df.merge(spy_df, on="date", how="inner")
        df.drop_duplicates(subset=["date"], inplace=True)

        if date not in df["date"].astype(str).values:
            logger.warning(f"Target date {date} not present in merged dataset.")
            return

        df["daily_return"] = df["index_value"].pct_change().fillna(0)
        df["spy_return"] = df["spy_close"].pct_change().fillna(0)
        df["cumulative_return"] = (1 + df["daily_return"]).cumprod() - 1
        df["rolling_volatility"] = df["daily_return"].rolling(7).std()
        df["rolling_beta_7d"] = df["daily_return"].rolling(7).corr(df["spy_return"])
        df["rolling_max"] = df["index_value"].cummax()
        df["drawdown"] = (df["index_value"] - df["rolling_max"]) / df["rolling_max"]
        df["drawdown_pct"] = df["drawdown"] * 100

        df["tickers"] = df.get("tickers", pd.Series([None] * len(df))).apply(
            lambda x: x.split(",") if isinstance(x, str) else []
        )

        turnover = [0]
        similarity = [1.0]
        for i in range(1, len(df)):
            prev = set(df["tickers"][i - 1])
            curr = set(df["tickers"][i])
            turnover.append(len(curr.symmetric_difference(prev)))
            similarity.append(
                len(curr & prev) / len(curr | prev) if curr | prev else 1.0
            )

        df["turnover"] = turnover
        df["exposure_similarity"] = similarity

        df_metrics = df[df["date"].astype(str) == date].copy()

        if df_metrics.empty:
            logger.warning(f"No data to compute metrics for {date}")
            return

        df_metrics = df_metrics[
            [
                "date",
                "index_value",
                "spy_close",
                "daily_return",
                "spy_return",
                "cumulative_return",
                "rolling_volatility",
                "rolling_beta_7d",
                "rolling_max",
                "drawdown",
                "drawdown_pct",
                "tickers",
                "turnover",
                "exposure_similarity",
            ]
        ]

        conn.execute("BEGIN")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS index_metrics (
                date DATE,
                index_value DOUBLE,
                spy_close DOUBLE,
                daily_return DOUBLE,
                spy_return DOUBLE,
                cumulative_return DOUBLE,
                rolling_volatility DOUBLE,
                rolling_beta_7d DOUBLE,
                rolling_max DOUBLE,
                drawdown DOUBLE,
                drawdown_pct DOUBLE,
                tickers TEXT,
                turnover INTEGER,
                exposure_similarity DOUBLE
            )
            """
        )

        conn.register("df_metrics", df_metrics)
        conn.execute("INSERT INTO index_metrics SELECT * FROM df_metrics")
        conn.unregister("df_metrics")
        conn.execute("COMMIT")

        logger.info(f"Stored metrics for {date} successfully.")

    except Exception as e:
        logger.error(f"Error computing metrics for {date}: {e}")
        try:
            conn.execute("ROLLBACK")
        except Exception:
            logger.warning("No active transaction to rollback.")
    finally:
        conn.close()
        logger.info("Finished computing daily metrics.")
