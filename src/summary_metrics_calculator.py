"""
Module: summary_metrics_calculator

Description:
------------
Computes rolling summary statistics for the custom equal-weighted index
based on a historical window ending on the specified target date.

Inputs:
-------
- date (str): End date in 'YYYY-MM-DD' format.
- Config.get_fetch_days(): Number of lookback days for window calculations.

Outputs:
--------
- Appends a single summary row to the `summary_metrics` DuckDB table.
  If insufficient data is present, inserts a row with null values.

Tables Involved:
----------------
- index_metrics (read)
- summary_metrics (write)

Metrics Computed:
-----------------
- Daily statistics (volatility, returns, Sharpe, Sortino, drawdown, etc.)
- Risk measures (VaR, Ulcer Index, skewness, kurtosis)
- Behavior metrics (win ratio, turnover, exposure similarity, capture ratios)
- Rebalancing metrics (rebalance count, exposure similarity)
- Best and worst days by return

Dependencies:
-------------
- duckdb
- pandas
- numpy
- scipy.stats
- src.config.Config
- src.logger.setup_logging
"""

from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import duckdb
from scipy.stats import skew, kurtosis

from src.config import Config
from src.logger import setup_logging

logger = setup_logging(Config.METRICS_LOG_FILE, logger_name="eqx.summary_metrics")


def max_consecutive_streak(series: pd.Series, positive: bool = True) -> int:
    """
    Calculate the longest streak of consecutive positive or negative values.

    Args:
        series (pd.Series): Time series of returns.
        positive (bool): If True, compute positive streaks; else negative.

    Returns:
        int: Maximum consecutive streak.
    """
    max_streak = current_streak = 0
    for val in series:
        if (positive and val > 0) or (not positive and val < 0):
            current_streak += 1
        else:
            max_streak = max(max_streak, current_streak)
            current_streak = 0
    return max(max_streak, current_streak)


def compute_summary_metrics(date: str) -> None:
    """
    Compute and store rolling summary metrics for a given end date.

    Args:
        date (str): Target end date in 'YYYY-MM-DD' format.

    Returns:
        None
    """
    logger.info(f"Computing summary metrics ending on {date}")

    if not Config.DUCKDB_FILE or not Path(Config.DUCKDB_FILE).exists():
        logger.error(f"DuckDB file not found at {Config.DUCKDB_FILE}")
        return

    conn = duckdb.connect(str(Config.DUCKDB_FILE))

    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS summary_metrics (
                date DATE,
                window_days INTEGER,
                best_day DATE,
                worst_day DATE,
                max_drawdown DOUBLE,
                final_return DOUBLE,
                avg_daily_return DOUBLE,
                volatility DOUBLE,
                sharpe_ratio DOUBLE,
                sortino_ratio DOUBLE,
                ulcer_index DOUBLE,
                annualized_return DOUBLE,
                annualized_volatility DOUBLE,
                up_capture DOUBLE,
                down_capture DOUBLE,
                win_ratio DOUBLE,
                avg_turnover DOUBLE,
                total_rebalances INTEGER,
                avg_exposure_similarity DOUBLE,
                var_95 DOUBLE,
                var_99 DOUBLE,
                return_skewness DOUBLE,
                return_kurtosis DOUBLE,
                max_gain_streak INTEGER,
                max_loss_streak INTEGER
            )
            """
        )

        end_date_dt = datetime.strptime(date, "%Y-%m-%d").date()
        start_date_dt = end_date_dt - timedelta(days=Config.get_fetch_days())

        df = conn.execute(
            f"""
            SELECT *
            FROM index_metrics
            WHERE date BETWEEN '{start_date_dt}' AND '{end_date_dt}'
            ORDER BY date
            """
        ).fetch_df()

        if df.empty or len(df) < 2:
            logger.warning(
                f"Insufficient data to compute summary metrics ending on {date}. Inserting NULL row."
            )
            summary = {
                col: None
                for col in [
                    "best_day",
                    "worst_day",
                    "max_drawdown",
                    "final_return",
                    "avg_daily_return",
                    "volatility",
                    "sharpe_ratio",
                    "sortino_ratio",
                    "ulcer_index",
                    "annualized_return",
                    "annualized_volatility",
                    "up_capture",
                    "down_capture",
                    "win_ratio",
                    "avg_turnover",
                    "total_rebalances",
                    "avg_exposure_similarity",
                    "var_95",
                    "var_99",
                    "return_skewness",
                    "return_kurtosis",
                    "max_gain_streak",
                    "max_loss_streak",
                ]
            }
            summary.update(
                {"date": str(end_date_dt), "window_days": Config.get_fetch_days()}
            )
        else:
            daily_return = df["daily_return"]
            final_return = (1 + daily_return).prod() - 1
            days = len(df)

            annualized_return = (
                (1 + final_return) ** (252 / days) - 1 if days > 0 else 0
            )
            annualized_volatility = daily_return.std() * np.sqrt(252)
            downside_std = daily_return[daily_return < 0].std()
            sortino_ratio = (
                daily_return.mean() / downside_std if downside_std > 0 else 0
            )
            ulcer_index = np.sqrt(np.mean(df["drawdown_pct"] ** 2))

            up_market = df[df["spy_return"] > 0]
            down_market = df[df["spy_return"] < 0]
            up_capture = (
                up_market["daily_return"].mean() / up_market["spy_return"].mean()
                if not up_market.empty
                else 0
            )
            down_capture = (
                down_market["daily_return"].mean() / down_market["spy_return"].mean()
                if not down_market.empty
                else 0
            )
            win_ratio = (daily_return > 0).mean()

            summary = {
                "date": str(end_date_dt),
                "window_days": Config.get_fetch_days(),
                "best_day": df.loc[daily_return.idxmax(), "date"],
                "worst_day": df.loc[daily_return.idxmin(), "date"],
                "max_drawdown": df["drawdown"].min(),
                "final_return": final_return,
                "avg_daily_return": daily_return.mean(),
                "volatility": daily_return.std(),
                "sharpe_ratio": (
                    daily_return.mean() / daily_return.std()
                    if daily_return.std() > 0
                    else 0
                ),
                "sortino_ratio": sortino_ratio,
                "ulcer_index": ulcer_index,
                "annualized_return": annualized_return,
                "annualized_volatility": annualized_volatility,
                "up_capture": up_capture,
                "down_capture": down_capture,
                "win_ratio": win_ratio,
                "avg_turnover": float(df["turnover"].mean()),
                "total_rebalances": int((df["turnover"] > 0).sum()),
                "avg_exposure_similarity": float(df["exposure_similarity"].mean()),
                "var_95": daily_return.quantile(0.05),
                "var_99": daily_return.quantile(0.01),
                "return_skewness": skew(daily_return, nan_policy="omit"),
                "return_kurtosis": kurtosis(daily_return, nan_policy="omit"),
                "max_gain_streak": max_consecutive_streak(daily_return, positive=True),
                "max_loss_streak": max_consecutive_streak(daily_return, positive=False),
            }

        summary_df = pd.DataFrame([summary])

        conn.execute("BEGIN")
        conn.execute(
            f"""
            DELETE FROM summary_metrics
            WHERE date = '{end_date_dt}' AND window_days = {Config.get_fetch_days()}
            """
        )
        conn.register("summary_df", summary_df)
        conn.execute("INSERT INTO summary_metrics SELECT * FROM summary_df")
        conn.unregister("summary_df")
        conn.execute("COMMIT")

        logger.info(
            f"Summary metrics stored for date: {date}, window_days: {Config.get_fetch_days()}"
        )

    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            logger.warning("No active transaction to rollback.")
        logger.error(f"Error computing summary metrics for {date}: {e}")
    finally:
        conn.close()
        logger.info("Summary metrics computation complete.")
