import os
import logging
import duckdb
import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis
from pathlib import Path

# --- Paths and Config ---
BASE_DIR = Path(__file__).resolve().parent.parent
DUCKDB_FILE = BASE_DIR / "eqx_index.db"
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "performance_metrics.log"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ],
    force=True
)

def max_consecutive_streak(series, positive=True):
    max_streak = 0
    current_streak = 0
    for val in series:
        if (positive and val > 0) or (not positive and val < 0):
            current_streak += 1
        else:
            max_streak = max(max_streak, current_streak)
            current_streak = 0
    return max(max_streak, current_streak)

def compute_metrics():
    logging.info("Starting performance metrics computation.")

    if not DUCKDB_FILE.exists():
        logging.error(f"DuckDB file not found at {DUCKDB_FILE}.")
        return

    conn = duckdb.connect(str(DUCKDB_FILE))
    try:
        index_df = conn.execute("SELECT * FROM index_values ORDER BY date").fetch_df()
        spy_df = conn.execute("SELECT * FROM market_index ORDER BY date").fetch_df()

        if index_df.empty:
            raise ValueError("index_values table is empty.")
        if spy_df.empty:
            raise ValueError("market_index table is empty.")

        df = index_df.merge(spy_df, on="date", how="inner")
        df.drop_duplicates(subset=["date"], keep="last", inplace=True)
        logging.info(f"Merged dataset has {len(df)} unique date rows.")

        # --- Return Metrics ---
        df['daily_return'] = df['index_value'].pct_change().fillna(0)
        df['spy_return'] = df['spy_close'].pct_change().fillna(0)
        df['cumulative_return'] = (1 + df['daily_return']).cumprod() - 1

        # --- Rolling Volatility & Beta ---
        df['rolling_volatility'] = df['daily_return'].rolling(window=7).std()
        df['rolling_beta_7d'] = df['daily_return'].rolling(window=7).corr(df['spy_return'])

        # --- Drawdowns ---
        df['rolling_max'] = df['index_value'].cummax()
        df['drawdown'] = (df['index_value'] - df['rolling_max']) / df['rolling_max']

        # --- Exposure Turnover ---
        turnover = [0]
        similarity = [1.0]
        tickers_raw = df.get('tickers', pd.Series([None] * len(df)))

        try:
            df['tickers'] = tickers_raw.apply(lambda x: x.split(',') if isinstance(x, str) else [])
            for i in range(1, len(df)):
                prev = set(df['tickers'][i - 1])
                curr = set(df['tickers'][i])
                added = curr - prev
                removed = prev - curr
                turnover.append(len(added.union(removed)))
                sim = len(prev & curr) / len(prev | curr) if prev | curr else 1.0
                similarity.append(sim)
        except Exception as e:
            logging.warning(f"Failed to compute turnover/exposure: {e}")
            turnover = [0] * len(df)
            similarity = [1.0] * len(df)

        df['turnover'] = turnover
        df['exposure_similarity'] = similarity

        logging.info("All rolling metrics and turnover/exposure calculated.")

        # --- Summary Statistics ---
        summary = {
            'best_day': df.loc[df['daily_return'].idxmax()]['date'],
            'worst_day': df.loc[df['daily_return'].idxmin()]['date'],
            'max_drawdown': df['drawdown'].min(),
            'final_return': df['cumulative_return'].iloc[-1],
            'avg_daily_return': df['daily_return'].mean(),
            'volatility': df['daily_return'].std(),
            'sharpe_ratio': (
                df['daily_return'].mean() / df['daily_return'].std()
                if df['daily_return'].std() > 0 else 0
            ),
            'avg_turnover': float(np.mean(turnover)),
            'total_rebalances': int(np.sum(np.array(turnover) > 0)),
            'avg_exposure_similarity': float(np.mean(similarity)),
            'var_95': df['daily_return'].quantile(0.05),
            'var_99': df['daily_return'].quantile(0.01),
            'return_skewness': skew(df['daily_return'], nan_policy='omit'),
            'return_kurtosis': kurtosis(df['daily_return'], nan_policy='omit'),
            'max_gain_streak': max_consecutive_streak(df['daily_return'], positive=True),
            'max_loss_streak': max_consecutive_streak(df['daily_return'], positive=False)
        }

        logging.info("Summary statistics compiled.")

        # --- Write to DuckDB ---
        conn.execute("BEGIN TRANSACTION")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_metrics AS SELECT * FROM df LIMIT 0
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_index_metrics ON index_metrics (date)")

        conn.register("df_metrics", df)
        conn.execute("""
            DELETE FROM index_metrics
            WHERE date IN (SELECT date FROM df_metrics)
        """)
        conn.execute("INSERT INTO index_metrics SELECT * FROM df_metrics")
        conn.unregister("df_metrics")

        summary_df = pd.DataFrame([summary])
        conn.register("summary_df", summary_df)
        conn.execute("CREATE OR REPLACE TABLE summary_metrics AS SELECT * FROM summary_df")
        conn.unregister("summary_df")

        conn.execute("COMMIT")
        logging.info("Metrics successfully stored in DuckDB tables.")
    except Exception as e:
        logging.error(f"Error computing metrics: {e}")
        conn.execute("ROLLBACK")
    finally:
        conn.close()
        logging.info("Performance metrics computation complete.")
