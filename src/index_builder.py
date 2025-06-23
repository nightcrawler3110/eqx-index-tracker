import duckdb
import pandas as pd
import logging
from pathlib import Path

# --- Config and Setup ---
BASE_DIR = Path(__file__).resolve().parent.parent
DUCKDB_FILE = BASE_DIR / "eqx_index.db"
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "index_builder.log"

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ],
    force=True
)

def build_index():
    logging.info("Starting index build process.")
    conn = duckdb.connect(str(DUCKDB_FILE))

    try:
        stock_df = conn.execute("SELECT * FROM stock_prices").fetch_df()
        logging.info(f"Fetched {len(stock_df)} rows from stock_prices.")

        if stock_df.empty:
            logging.warning("stock_prices table is empty. Aborting index build.")
            return

        dates_df = conn.execute("SELECT DISTINCT date FROM stock_prices ORDER BY date").fetch_df()
        logging.info(f"Found {len(dates_df)} distinct dates to process.")

        index_data = []

        for date in dates_df['date']:
            try:
                top_100_df = conn.execute(f"""
                    SELECT ticker, close
                    FROM stock_prices
                    WHERE date = '{date}'
                    ORDER BY market_cap DESC
                    LIMIT 100
                """).fetch_df()

                if len(top_100_df) < 100:
                    logging.warning(f"Skipping {date} — less than 100 valid tickers.")
                    continue

                equal_weight = 1 / 100
                index_value = (top_100_df['close'] * equal_weight).sum()

                spy_df = conn.execute(f"""
                    SELECT spy_close FROM market_index
                    WHERE date = '{date}'
                """).fetch_df()

                spy_value = spy_df['spy_close'].iloc[0] if not spy_df.empty else None

                index_data.append({
                    'date': date,
                    'index_value': round(index_value, 4),
                    'spy_value': round(spy_value, 4) if spy_value is not None else None,
                    'tickers': ','.join(top_100_df['ticker'].tolist())
                })
            except Exception as e:
                logging.error(f"Error processing date {date}: {e}")

        df_index = pd.DataFrame(index_data)
        if df_index.empty:
            logging.error("No index values calculated. Aborting.")
            return

        # --- Safe upsert with DuckDB-compatible deletion ---
        conn.execute("BEGIN TRANSACTION")

        # Create table if not exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_values (
                date DATE,
                index_value DOUBLE,
                spy_value DOUBLE,
                tickers TEXT
            )
        """)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_index_values ON index_values (date)")

        conn.register("df_index", df_index)

        # DELETE + INSERT (safe upsert)
        conn.execute("""
            DELETE FROM index_values
            WHERE date IN (SELECT date FROM df_index)
        """)
        conn.execute("""
            INSERT INTO index_values
            SELECT * FROM df_index
        """)
        conn.unregister("df_index")

        conn.execute("COMMIT")
        logging.info(f"index_values table updated with {len(df_index)} rows.")
    except Exception as e:
        logging.error(f"Failed to build index: {e}")
        conn.execute("ROLLBACK")
    finally:
        conn.close()
        logging.info("Index build process completed.")
