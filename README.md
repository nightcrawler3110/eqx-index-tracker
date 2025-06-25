# 📈 Equal Weighted Index Builder & Analytics Platform

## Overview

This project builds a **equal-weighted stock index** of the top 100 US stocks by market capitalization, benchmarked against the SPY ETF. It performs automated data ingestion, index construction, performance metric computation, and provides interactive visualizations via Streamlit.

---

## ✨ Features


🧺 Equal-Weighted Index Logic  
    - Dynamically selects the top 100 US stocks by market cap every day.
    - Ensures the index reflects up-to-date market conditions.

⚡️ High-Performance Data Ingestion  
    - Supports both historical and daily fetching using `yfinance`.
    - Features include:
        • Parallel API calls
        • Retry + fallback logic
        • Ticker-level error handling and logging

📦 Daily + Historical Pipeline Support  
    - `run_daily_batch.sh`: Cron-ready pipeline for daily ingestion, index building, metrics, and export.
    - `run_historical_pipeline.py`: One-time bootstrap/backfill for past data.

📉 Advanced Performance & Risk Metrics  
    - Computes detailed analytics including:
        • Daily/Annual Returns
        • CAGR, Sharpe, Sortino Ratios
        • Rolling Volatility, Max Drawdown
        • Beta, Value-at-Risk (VaR), performance streaks

🧠 Summary + Daily Metrics Separation  
    - Modular design to isolate:
        • Daily insights (volatility, beta, drawdown, turnover)
        • Long-term summaries (return profile, risk ratios, stability)

📊 Streamlit Dashboards  
    - Interactive UI for exploring:
        • EQX vs SPY performance
        • Drawdowns, normalized returns, rolling metrics
        • Top tickers & weights
        • Validation alerts (missing/null/extreme values)

📄 Excel Export  
    - Structured reports including:
        • Index value and composition
        • Day-over-day changes
        • Summary and validation metrics

🔢 Modular Codebase  
    - Built for production with:
        • Centralized config & logging
        • Clean folder structure
        • Pytest test suite with mocking

---

## 🧱 Project Structure

```text
eqx-index-project/
│
├── data/                          # DuckDB database lives here
│   └── eqx_index.db
│
├── export/                        # Excel exports (daily snapshots)
│   └── eqx_index_export_*.xlsx
│
├── logs/                          # Logs for all modules
├── mock_out/                      # Output for test mocking
├── reports/detailed_issues/      # Validation/QA reports
│
├── src/                           # Core pipeline modules
│   ├── config.py                  # Central configuration
│   ├── logger.py                  # Logging setup
│   ├── data_ingestion.py         # Fetch & store historical + daily stock data
│   ├── data_validation.py        # Validation checks on ingested data
│   ├── index_builder.py          # Builds and appends index values
│   ├── daily_metrics_calculator.py     # Daily return, volatility, beta etc.
│   ├── summary_metrics_calculator.py   # Long-term analytics
│   ├── excel_exporter.py         # Full Excel export pipeline
│   ├── visualize_analytics_report.py   # Streamlit dashboard (EQX vs SPY)
│   ├── visualize_validation_issues.py # Dashboard for data issues
│
├── tests/                         # Unit tests (pytest)
│   ├── test_*.py
│
├── run_daily_batch.sh            # Daily production script (cron)
├── run_historical_pipeline.py    # One-time historical data bootstrap
│
├── inspect_duck_db.py            # CLI for exploring the DuckDB
├── eqx_runner.py                 # Modular runner (for manual runs)
├── failed_tickers.csv            # Logging failures during ingestion
├── requirements.txt              # Project dependencies
└── README.md                     # You're looking at it!

```

## 🏗️ Architecture

```markdown

           ┌────────────────────┐
           │   Data Ingestion   │
           │  (yfinance, API)   │
           └────────┬───────────┘
                    ▼
           ┌────────────────────┐
           │   DuckDB Storage   │
           │   eqx_index.db     │
           └────────┬───────────┘
                    ▼
         ┌────────────────────────┐
         │   Index Construction   │
         │ (Top 100 EQ-weighted)  │
         └────────┬───────────────┘
                  ▼
        ┌────────────────────────────┐
        │  Performance Metrics Calc  │
        │ (Sharpe, Drawdown, etc.)   │
        └────────┬───────────────────┘
                 ▼
        ┌──────────────────────────┐
        │    Data Validation       │
        │  (missing, negatives)    │
        └────────┬────────────────-┘
                 ▼
       ┌──────────────────────────────┐
       │  Excel Export (index_metrics)│
       └────────┬─────────────────────┘
                 ▼
       ┌──────────────────────────────┐
       │ Streamlit Visualization      │
       │ - Index Analytics            │
       │ - Validation Alerts          │
       └──────────────────────────────┘
                 ▲
                 │
      ┌───────────────────────────────┐
      │   Logging + Unit Testing      │
      │ (Pytest + Custom Logs)        │
      └───────────────────────────────┘

```

---

## 🧩 Project Modules

| Module/File                              | Description                                                                                                                      |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **`src/config.py`**                      | Central config management for paths, constants, filenames, and parameters.                                                       |
| **`src/logger.py`**                      | Sets up structured, rotating logs with named loggers per module.                                                                 |
| **`src/data_ingestion.py`**              | Fetches historical + daily stock and SPY prices via `yfinance`. Includes parallelism, retries, and ticker-level logging.         |
| **`src/data_validation.py`**             | Validates stock data for nulls, negatives, missing dates, and extreme changes. Stores results in `detailed_issues`.              |
| **`src/index_builder.py`**               | Selects top 100 tickers by market cap, builds equal-weighted index, and appends to `index_values`.                               |
| **`src/daily_metrics_calculator.py`**    | Computes daily return, volatility, drawdown, beta, turnover, and exposure similarity. Saves to `index_metrics`.                  |
| **`src/summary_metrics_calculator.py`**  | Calculates long-term metrics: Sharpe, Sortino, CAGR, max drawdown, VaR, and winning/losing streaks. Stored in `summary_metrics`. |
| **`src/excel_exporter.py`**              | Exports index data (performance, composition, changes, metrics) to Excel. Sheet-wise formatting and safe file writing.           |
| **`src/visualize_analytics_report.py`**  | Streamlit app to explore EQX vs SPY, normalized returns, volatility, top tickers, and rebalancing.                               |
| **`src/visualize_validation_issues.py`** | Streamlit dashboard for inspecting validation issues and anomalies in ingested data.                                             |
| **`src/eqx_runner.py`**                  | Central runner that coordinates ingestion, index building, metrics, and export. Great for CLI/manual usage.                      |
| **`src/inspect_duck_db.py`**             | Quick script to inspect DuckDB tables and run ad hoc queries.                                                                    |

Utility Scripts

| Script/File                  | Purpose                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------- |
| `run_daily_batch.sh`         | Production cron job runner for the daily pipeline (ingestion → index → metrics → export). |
| `run_historical_pipeline.py` | Bootstrapper for historical data ingestion and index backfilling.                         |
| `failed_tickers.csv`         | Logs failed tickers during data ingestion for review and retry.                           |

---

## 🧪 DuckDB Schema

| Table Name        | Purpose                                          |
|-------------------|--------------------------------------------------|
| `stock_prices`    | Daily OHLCV prices + market cap for each ticker  |
| `market_index`    | Daily SPY OHLCV prices                           |
| `index_values`    | Daily equal-weighted index + SPY close           |
| `index_metrics`   | Daily return, volatility, Sharpe ratio, etc.     |
| `summary_metrics` | Project-wide stats like max drawdown, CAGR, etc. |

---
---

## 📊 Metrics Computed


- `daily_return`: EQX return from previous day
- `spy_return`: SPY return from previous day
- `cumulative_return`: EQX return since start of 7-day window
- `rolling_volatility`: 7-day rolling standard deviation of EQX returns
- `rolling_beta_7d`: 7-day rolling correlation between EQX and SPY returns
- `rolling_max`: Maximum EQX index value so far (for drawdown calculation)
- `drawdown`: Difference between current and peak EQX value
- `drawdown_pct`: Percentage drawdown from peak
- `tickers`: List of top 100 tickers used in index on the given day
- `turnover`: Number of tickers changed since previous day
- `exposure_similarity`: Jaccard similarity of tickers with previous day

- `window_days`: Number of lookback days for summary metrics
- `best_day`: Date with highest EQX daily return in window
- `worst_day`: Date with lowest EQX daily return in window
- `max_drawdown`: Maximum drawdown value over the window
- `final_return`: Cumulative EQX return over the window
- `avg_daily_return`: Mean of EQX daily returns in window
- `volatility`: Standard deviation of EQX daily returns in window
- `sharpe_ratio`: Ratio of average return to volatility
- `sortino_ratio`: Ratio of average return to downside volatility
- `ulcer_index`: Root mean square of drawdown percentages
- `annualized_return`: Projected annual return based on window
- `annualized_volatility`: Projected annual volatility
- `up_capture`: EQX return capture ratio during SPY up days
- `down_capture`: EQX return capture ratio during SPY down days
- `win_ratio`: Proportion of days with positive return
- `avg_turnover`: Average daily turnover (ticker changes)
- `total_rebalances`: Number of rebalancing days (non-zero turnover)
- `avg_exposure_similarity`: Average similarity to previous day's tickers
- `var_95`: 5th percentile of daily returns (Value-at-Risk 95%)
- `var_99`: 1st percentile of daily returns (Value-at-Risk 99%)
- `return_skewness`: Skewness of daily return distribution
- `return_kurtosis`: Kurtosis of daily return distribution
- `max_gain_streak`: Longest consecutive streak of positive returns
- `max_loss_streak`: Longest consecutive streak of negative returns


---
---

## 🚀 Getting Started

```bash
# 1. Clone the repository
$ git clone https://github.com/your-username/eqx-index-project.git
$ cd eqx-index-project

# 2. Set up virtual environment
$ python -m venv venv
$ source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Install dependencies
$ pip install -r requirements.txt
```

### Prerequisites
- Python 3.9+
- yfinance, duckdb, pandas, numpy, streamlit, plotly, scipy, openpyxl, pytest

---

"""
🏠 EQX Runner – Command-Line Pipeline Entry
===========================================

Module: eqx_runner.py

Description:
------------
Main CLI entry point for the EQX index pipeline. Supports step-by-step or full
execution of the entire workflow: data ingestion, index construction, metric computation,
data validation, and Excel export.

Supported Steps (--steps):
--------------------------
- ingest_data             : Ingest stock & SPY data for the given date
- build_index             : Construct equal-weighted index from top 100 market cap stocks
- compute_daily_metrics   : Compute per-day metrics (return, volatility, drawdown, turnover, etc.)
- compute_summary_metrics : Compute summary stats (Sharpe, Sortino, CAGR, streaks, VaR, etc.)
- validate_data           : Run validations on index values and computed metrics
- export_excel            : Export results to a clean Excel report
- run_all                 : Execute all steps sequentially

Usage Example:
--------------
Run full pipeline for a specific date with a 30-day summary window and Excel export:

    python eqx_runner.py --steps run_all --date 2024-06-25 --window 30 --excel_output_dir /path/to/output

Arguments:
----------
--steps            List of steps to run (space-separated if multiple)
--date             Target date in YYYY-MM-DD format
--window           Lookback window (in days) for summary metrics (default: 40)
--excel_output_dir Output directory for Excel export (optional)


---

## 📊 Visualize with Streamlit

### 🔍 Analytics Dashboard
```bash
streamlit run src/visualize_analytics_report.py
```
Includes:
- EQX Index vs SPY chart
- Cumulative return
- Rolling volatility & beta
- Top tickers table
- Exposure similarity
- Turnover tracker

### ⚠️ Validation Dashboard
```bash
streamlit run src/visualize_validation_alerts.py
```
Includes:
- Missing/null/negative values
- Abnormal price spikes
- Summary of validation issues

---

"""
✅ Running Unit Tests
----------------------

Run the full test suite using:

    pytest

Test Coverage Includes:
------------------------

- Data Ingestion
    • Parallel fetching, retry logic, ticker filtering, failure logging
- Index Construction
    • Top 100 selection by market cap, equal-weighted index value calculation
- Daily Metrics Computation
    • Returns, volatility, drawdowns, turnover, exposure similarity
- Summary Metrics Calculation
    • Sharpe, Sortino, VaR, Ulcer Index, streaks, capture ratios
- Excel Exporting
    • Composition breakdowns, performance metrics, clean sheet formatting
- Mocking & Isolation
    • All external dependencies (DuckDB, ExcelWriter, APIs) are fully mocked
"""


## 📁 Outputs

- `eqx_index.db` – DuckDB database
- `export/ – Excel exports
- `logs/` – Logs per module
- Streamlit dashboards – Interactive exploration

---

## ⚙️ Design Choices

- **Modular Architecture**: Each component of the ETL pipeline (ingestion, index building, metrics, validations, export, visualization) is separated into its own module, ensuring clarity, testability, and reusability.
- **DuckDB Backend**: Chosen for its high-performance SQL querying and in-memory speed without requiring external setup—perfect for local analytical tasks.
- **Daily Rebalancing**: Ensures the top 100 are always up-to-date based on current market cap, reflecting real-world index behavior.
- **Parallel Data Fetching**: Uses concurrent threads to reduce wait times when pulling large volumes of stock data from yfinance.
- **Streamlit for Visualization**: Enables quick deployment of interactive dashboards without setting up frontend frameworks.
- **Production-Grade Logging**: Every script uses a shared logger with modular file outputs and console logging for easy debugging and audit trails.
- **Data Validations**: Built-in checks for missing, null, negative, or extreme values to ensure clean downstream processing.

---

## Acknowledgements
- yfinance – Stock price data
- DuckDB – Local OLAP SQL engine
- Streamlit – Visualization platform
- pandas, numpy, scipy – Data science toolkit

