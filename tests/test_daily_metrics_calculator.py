import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.daily_metrics_calculator import compute_daily_metrics

# ------------------------
# Helper Fixtures
# ------------------------


@pytest.fixture
def dummy_index_data():
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "index_value": [1000, 1020, 1040],
            "tickers": ["AAPL,MSFT", "GOOGL,MSFT", "AAPL,GOOGL"],
        }
    )


@pytest.fixture
def dummy_spy_data():
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "spy_value": [400, 408, 412],
        }
    )


# ------------------------
# Main Test
# ------------------------


@patch("src.daily_metrics_calculator.duckdb.connect")
@patch("src.daily_metrics_calculator.Path.exists", return_value=True)
@patch("src.daily_metrics_calculator.Config")
def test_compute_daily_metrics_happy_path(
    mock_config, mock_exists, mock_connect, dummy_index_data, dummy_spy_data
):
    mock_config.DUCKDB_FILE = "mocked.duckdb"

    # Fake connection and cursor
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Side effect for DuckDB queries
    def execute_side_effect(query):
        if "FROM index_values" in query:
            return MagicMock(fetch_df=lambda: dummy_index_data)
        elif "FROM market_index" in query:
            return MagicMock(fetch_df=lambda: dummy_spy_data)
        else:
            return MagicMock()

    mock_conn.execute.side_effect = execute_side_effect

    # Run
    compute_daily_metrics("2024-01-03")

    # Assert inserts called
    insert_calls = [
        c
        for c in mock_conn.execute.call_args_list
        if "INSERT INTO index_metrics" in str(c)
    ]
    assert insert_calls, "Expected INSERT INTO index_metrics to be called"
