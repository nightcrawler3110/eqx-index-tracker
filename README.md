# 📈 EQX Index Builder & Analytics Platform

## Overview

This project builds a **custom equal-weighted stock index** of the top 100 US stocks by market capitalization, benchmarked against the SPY ETF. It performs automated data ingestion, index construction, performance metric computation, and provides interactive visualizations via Streamlit.

---

## ✨ Features

- 🧺 **Equal-Weighted Index**: Dynamically selects top 100 US stocks by market cap daily.
- ⚡️ **Fast Data Ingestion**: Historical stock and SPY price fetching via `yfinance`, with retry and fallback logic.
- 🧠 **Advanced Metrics**: Computes Sharpe Ratio, Volatility, Drawdown, CAGR, Sortino, VaR, streaks, and more.
- 📊 **Optimized SQL Backend**: Built on **DuckDB** for blazing-fast local analytical queries.
- 🔮 **Robust Validations**: Identifies missing, null, or extreme values before corrupting metrics.
- 📈 **Streamlit Dashboards**:
    - Index vs SPY time-series
    - Rolling returns, drawdowns
    - Validation alert inspector
    - Top tickers and composition breakdown
- 📄 **Excel Export**: Clean report with performance metrics, composition, changes, and summary.
- 🔢 **Modular Codebase**: Production-grade structure with reusable configs, logging, and tests.

---

## 🧱 Project Structure

```text
eqx-index-project/
├── main.py                             # Orchestrates full ETL pipeline
├── src/
│   ├── config.py                       # Central configuration (paths, filenames)
│   ├── logger.py                       # Shared logging setup
│   ├── data_ingestion.py               # Fetches stock & SPY data from yfinance
│   ├── index_builder.py                # Builds equal-weighted index daily
│   ├── performance_metrics.py          # Computes returns, drawdowns, Sharpe, etc.
│   ├── data_validations.py             # Validates nulls, negatives, and spikes
│   ├── excel_exporter.py               # Exports metrics and composition to Excel
│   ├── visualize_analytics_report.py   # Streamlit dashboard for performance
│   ├── visualize_validation_alerts.py  # Streamlit dashboard for validation issues
│   ├── inspect_duck_db.py              # Explore contents of DuckDB interactively
│   └── __init__.py
├── tests/                              # Unit tests for each major component
│   ├── test_ingestion.py
│   ├── test_metrics.py
│   ├── test_validation.py
│   └── ...
├── logs/                                # Logs for each module
│   ├── ingestion.log
│   ├── metrics.log
│   └── ...
├── export/
│   └── index_metrics.xlsx              # Final Excel report
├── data/
│   └── eqx_index.db                    # DuckDB database (generated)
├── requirements.txt
└── README.md                           # Project documentation (for GitHub)

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

| Module                          | Description                                                                                                       |
|----------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `data_ingestion.py`             | Fetches historical data for US stocks and SPY using `yfinance`. Supports parallel fetching with retry logic.      |
| `index_builder.py`              | Computes the equal-weighted index daily using the top 100 stocks by market cap.                                   |
| `performance_metrics.py`        | Computes key metrics like daily returns, Sharpe ratio, volatility, drawdown, CAGR, and return streaks.            |
| `data_validations.py`           | Validates computed results for missing values, negative prices, and anomalies. Stores alerts for inspection.       |
| `excel_exporter.py`             | Exports final index metrics and summary statistics into a well-formatted Excel file.                               |
| `visualize_analytics_report.py` | Interactive dashboard to visualize index performance, SPY comparison, and cumulative returns.                      |
| `visualize_validation_alerts.py`| Streamlit dashboard to explore validation errors, missing values, or outliers.                                     |
| `config.py`                     | Centralized configuration file for file paths, constants, and DuckDB file location.                                |
| `logger.py`                     | Unified logging setup with modular log files per script. Supports both console and file logging.                   |
| `main.py`                       | Main orchestrator script to run the full ETL pipeline in sequence.                                                 |
| `inspect_duck_db.py`            | Utility script to inspect DuckDB tables, schemas, and sample rows for debugging.                                   |
| `tests/`                        | Unit tests for each major component using `pytest` for validation and correctness.                                 |

---
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

- Total Return / CAGR
- Daily & Cumulative Return
- Annualized Volatility
- Sharpe & Sortino Ratios
- Drawdowns & Ulcer Index
- Value-at-Risk (VaR 95%, 99%)
- Rolling Beta (7D)
- Turnover & Exposure Similarity
- Win Ratio, Up/Down Market Capture
- Longest Gain/Loss Streaks

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

## 🏠 Running the Pipeline

```bash
python main.py
```
This will:
- Ingest historical stock + SPY data
- Compute index values + metrics
- Run validations
- Export to Excel

---

## 📊 Visualize with Streamlit

### 🔍 Analytics Dashboard
```bash
streamlit run visualize_analytics_report.py
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
streamlit run visualize_validation_alerts.py
```
Includes:
- Missing/null/negative values
- Abnormal price spikes
- Summary of validation issues

---

## ✅ Running Unit Tests

```bash
pytest
```
Covers:
- Data ingestion
- Index construction
- Metrics computation
- Excel exporting
- Validations

---

## 📁 Outputs

- `eqx_index.db` – DuckDB database
- `export/index_metrics.xlsx` – Excel export
- `logs/` – Logs per module
- Streamlit dashboards – Interactive exploration

---

## 🧠 Key Concepts

- **Equal-Weighted Index**: Each day selects top 100 stocks, equally weighted
- **Daily Rebalancing**: Reflects changes in top 100 by market cap
- **DuckDB**: In-process analytics engine with high-performance SQL
- **Composable Modules**: All functionality split into reusable units

---

## 🙏 Acknowledgements
- yfinance – Stock price data
- DuckDB – Local OLAP SQL engine
- Streamlit – Visualization platform
- pandas, numpy, scipy – Data science toolkit

"""