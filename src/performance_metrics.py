"""
Module: performance_metrics

Overview:
---------
This module computes and stores a comprehensive set of performance metrics for a custom equal-weighted stock index,
benchmarked against the SPY market index. It reads daily index values and SPY prices from DuckDB tables, performs
detailed financial analysis, and writes both granular and summary-level results back to the database.

Key Responsibilities:
---------------------
- Read index values from `index_values` and SPY index prices from `market_index`.
- Calculate:
    • Daily/cumulative returns
    • Rolling volatility and beta
    • Drawdowns
    • Turnover and exposure similarity
    • Advanced metrics: Sharpe/Sortino, ulcer index, streaks, skew, kurtosis, VaR
- Write full metrics to `index_metrics` and summary metrics to `summary_metrics`.

Dependencies:
-------------
- pandas, numpy, duckdb, scipy.stats
- src.config.Config
- src.logger.setup_logging

Author: Shaily
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis

from src.config import Config
from src.logger import setup_logging

# Initialize logging
setup_logging(Config.METRICS_LOG_FILE)


def max_consecutive_streak(series: pd.Series, positive: bool = True) -> int:
    """Calculate the longest consecutive streak of positive or negative returns."""
    max_streak = current_streak = 0
    for val in series:
        if (positive and val > 0) or (not positive and val < 0):
            current_streak += 1
        else:
            max_streak = max(max_streak, current_streak)
            current_streak = 0
    return max(max_streak, current_streak)


def compute_metrics() -> None:
    """Compute and store index performance metrics and summary stats."""
    logging.info("Starting performance metrics computation.")

    if not Config.DUCKDB_FILE or not Path(Config.DUCKDB_FILE).exists():
        logging.error(f"DuckDB file not found at {Config.DUCKDB_FILE}")
        return

    conn = duckdb.connect(str(Config.DUCKDB_FILE))

    try:
        index_df = conn.execute("SELECT * FROM index_values ORDER BY date").fetch_df()
        spy_df = conn.execute("SELECT * FROM market_index ORDER BY date").fetch_df()
        spy_df.rename(columns={"spy_value": "spy_close"}, inplace=True)

        if index_df.empty:
            raise ValueError("index_values table is empty.")
        if spy_df.empty:
            raise ValueError("market_index table is empty.")

        df = index_df.merge(spy_df, on="date", how="inner")
        df.drop_duplicates(subset=["date"], keep="last", inplace=True)
        logging.info(f"Merged dataset has {len(df)} rows.")

        # --- Daily returns and cumulative ---
        df['daily_return'] = df['index_value'].pct_change().fillna(0)
        df['spy_return'] = df['spy_close'].pct_change().fillna(0)
        df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1
        df['rolling_volatility'] = df['daily_return'].rolling(7).std()
        df['rolling_beta_7d'] = df['daily_return'].rolling(7).corr(df['spy_return'])

        # --- Drawdown ---
        df['rolling_max'] = df['index_value'].cummax()
        df['drawdown'] = (df['index_value'] - df['rolling_max']) / df['rolling_max']
        df['drawdown_pct'] = df['drawdown'] * 100

        # --- Turnover and exposure similarity ---
        turnover = [0]
        similarity = [1.0]
        df['tickers'] = df.get('tickers', pd.Series([None] * len(df))).apply(
            lambda x: x.split(',') if isinstance(x, str) else []
        )

        for i in range(1, len(df)):
            prev = set(df['tickers'][i - 1])
            curr = set(df['tickers'][i])
            turnover.append(len(curr.symmetric_difference(prev)))
            similarity.append(len(curr & prev) / len(curr | prev) if curr | prev else 1.0)

        df['turnover'] = turnover
        df['exposure_similarity'] = similarity

        # --- Summary stats ---
        days = len(df)
        daily_return = df['daily_return']
        final_return = df['cumulative_return'].iloc[-1] if not df.empty else 0

        annualized_return = (1 + final_return) ** (252 / days) - 1 if days > 0 else 0
        annualized_volatility = daily_return.std() * np.sqrt(252)
        downside_std = daily_return[daily_return < 0].std()
        sortino_ratio = daily_return.mean() / downside_std if downside_std > 0 else 0
        ulcer_index = np.sqrt(np.mean(df['drawdown_pct'] ** 2)) if not df.empty else 0

        up_market = df[df['spy_return'] > 0]
        down_market = df[df['spy_return'] < 0]
        up_capture = up_market['daily_return'].mean() / up_market['spy_return'].mean() if not up_market.empty else 0
        down_capture = down_market['daily_return'].mean() / down_market['spy_return'].mean() if not down_market.empty else 0
        win_ratio = (daily_return > 0).mean()

        summary = {
            'best_day': df.loc[daily_return.idxmax(), 'date'] if not df.empty else None,
            'worst_day': df.loc[daily_return.idxmin(), 'date'] if not df.empty else None,
            'max_drawdown': df['drawdown'].min(),
            'final_return': final_return,
            'avg_daily_return': daily_return.mean(),
            'volatility': daily_return.std(),
            'sharpe_ratio': daily_return.mean() / daily_return.std() if daily_return.std() > 0 else 0,
            'sortino_ratio': sortino_ratio,
            'ulcer_index': ulcer_index,
            'annualized_return': annualized_return,
            'annualized_volatility': annualized_volatility,
            'up_capture': up_capture,
            'down_capture': down_capture,
            'win_ratio': win_ratio,
            'avg_turnover': float(np.mean(turnover)),
            'total_rebalances': int(np.sum(np.array(turnover) > 0)),
            'avg_exposure_similarity': float(np.mean(similarity)),
            'var_95': daily_return.quantile(0.05),
            'var_99': daily_return.quantile(0.01),
            'return_skewness': skew(daily_return, nan_policy='omit'),
            'return_kurtosis': kurtosis(daily_return, nan_policy='omit'),
            'max_gain_streak': max_consecutive_streak(daily_return, positive=True),
            'max_loss_streak': max_consecutive_streak(daily_return, positive=False)
        }

        # --- Ensure column selection is aligned with DB schema ---
        index_metrics_cols = [
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
            "exposure_similarity"
        ]
        df_metrics = df[index_metrics_cols].copy()
        summary_df = pd.DataFrame([summary])

        # --- Store results ---
        conn.execute("BEGIN")

        conn.execute("""
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
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS summary_metrics (
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
        """)

        conn.register("df_metrics", df_metrics)
        conn.execute("INSERT INTO index_metrics SELECT * FROM df_metrics")
        conn.unregister("df_metrics")

        conn.execute("DELETE FROM summary_metrics")
        conn.register("summary_df", summary_df)
        conn.execute("INSERT INTO summary_metrics SELECT * FROM summary_df")
        conn.unregister("summary_df")

        conn.execute("COMMIT")
        logging.info("Metrics successfully stored in DuckDB.")

    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            logging.warning("No active transaction to rollback.")
        logging.error(f"Error computing metrics: {e}")

    finally:
        conn.close()
        logging.info("Performance metrics computation complete.")
