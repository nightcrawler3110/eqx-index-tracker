# main.py

from src.data_ingestion import run_ingestion
from src.index_builder import build_index
from src.analytics import compute_metrics
from src.excel_exporter import export_to_excel
from src.data_validations import run_validations 


def main():
    print("Step 1: Fetching and storing stock data...")
    run_ingestion()
    print("Data ingestion completed.\n")

    print("Step 2: Building equal-weighted index...")
    build_index()
    print("Index built and validated.\n")

    print("Step 3: Computing performance metrics...")
    compute_metrics()
    print("Analytics computed and validated.\n")

    print("Step 4: Running data validations...")
    run_validations()
    print("Data validations completed.\n")

    print("Step 5: Exporting to Excel...")
    export_to_excel()
    print("Data exported to Excel.\n")

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
