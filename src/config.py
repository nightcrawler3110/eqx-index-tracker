"""
Configuration module for the EQX Index Project.

This module sets up and manages core directory paths, environment variables, and file locations
used throughout the project. It ensures that required directories exist and provides a Config
class with centralized access to configuration values such as database file paths, API keys,
log files, report files, and data output locations.

Attributes:
    BASE_DIR (Path): The root directory of the project.
    LOGS_DIR (Path): Directory for storing log files.
    REPORTS_DIR (Path): Directory for storing report files.
    DETAILED_ISSUES_DIR (Path): Directory for storing detailed issue reports.
    DATA_DIR (Path): Directory for storing data output files.

Classes:
    Config: Contains static configuration values and file paths, sourced from environment
        variables or default locations within the project structure.
"""

import os
from pathlib import Path

# --- Base Directories ---
BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"
DETAILED_ISSUES_DIR = REPORTS_DIR / "detailed_issues"
DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = BASE_DIR / "export"

# --- Ensure Directories Exist ---
for directory in [LOGS_DIR, REPORTS_DIR, DETAILED_ISSUES_DIR, DATA_DIR, EXPORT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

class Config:
    # --- Environment and Core Files ---
    DUCKDB_FILE = os.getenv("DUCKDB_FILE", str(DATA_DIR / "eqx_index.db"))
    FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
    FETCH_DAYS = int(os.getenv("FETCH_DAYS", "30"))
    FAILED_TICKERS_FILE = BASE_DIR / "failed_tickers.csv"

    # --- Logs ---
    LOGS_DIR = LOGS_DIR
    INGESTION_LOG_FILE = LOGS_DIR / "ingestion.log"
    INDEX_BUILDER_LOG_FILE = LOGS_DIR / "index_builder.log"
    METRICS_LOG_FILE = LOGS_DIR / "performance_metrics.log"
    VALIDATION_LOG_FILE = LOGS_DIR / "data_validation.log"
    EXCEL_EXPORT_LOG_FILE = LOGS_DIR / "export_excel.log"
    LOG_FILE = INGESTION_LOG_FILE  # Default log used by setup_logging()

    # --- Output Reports ---
    REPORTS_DIR = REPORTS_DIR
    DETAILED_ISSUES_DIR = DETAILED_ISSUES_DIR
    VALIDATION_REPORT = REPORTS_DIR / "data_validation_report.csv"

    # --- Excel file Output ---
    EXCEL_OUTPUT_FILE = EXPORT_DIR / "equal_weight_index.xlsx"

    # --- Base ---
    BASE_DIR = BASE_DIR
