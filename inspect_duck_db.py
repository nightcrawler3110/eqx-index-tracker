# inspect_duckdb.py

import duckdb
import pandas as pd

DUCKDB_FILE = "eqx_index.db"

def inspect_duckdb(db_file=DUCKDB_FILE):
    conn = duckdb.connect(db_file)
    print(f"Inspecting DuckDB file: {db_file}\n")

    # Get all table names
    tables = conn.execute("SHOW TABLES").fetchall()
    if not tables:
        print("No tables found.")
        return

    for t in tables:
        table = t[0]
        print(f"\nTable: {table}")

        # Schema
        try:
            schema_df = conn.execute(f"DESCRIBE {table}").fetchdf()
            print("Schema:")
            print(schema_df.to_string(index=False))
        except Exception as e:
            print(f"Failed to get schema: {e}")
            continue

        # Row count
        try:
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"Row count: {row_count}")
        except Exception as e:
            print(f"Failed to count rows: {e}")
            continue

        # Sample rows
        try:
            sample = conn.execute(f"SELECT * FROM {table} LIMIT 5").fetchdf()
            print("Sample rows:")
            print(sample.to_string(index=False))
        except Exception as e:
            print(f"Failed to fetch sample rows: {e}")
            continue

    conn.close()

if __name__ == "__main__":
    inspect_duckdb()
 