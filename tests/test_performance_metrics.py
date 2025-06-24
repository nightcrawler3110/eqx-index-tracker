import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime

from src import performance_metrics


@pytest.fixture
def mock_dataframes():
    index_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5),
            "index_value": [100, 102, 101, 103, 105],
            "tickers": ["A,B,C"] * 5,
        }
    )
    spy_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5),
            "spy_value": [4000, 4020, 4010, 4050, 4100],
        }
    )
    return index_df, spy_df


@patch("src.performance_metrics.Path")
@patch("src.performance_metrics.Config")
@patch("src.performance_metrics.logger")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_success(
    mock_connect, mock_logger, mock_config, mock_path, mock_dataframes
):
    index_df, spy_df = mock_dataframes

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Fake DB path exists
    mock_config.DUCKDB_FILE = "mock.db"
    mock_path.return_value.exists.return_value = True

    # First two calls to fetch_df for index_df and spy_df
    def fetch_df_side_effect():
        if fetch_df_side_effect.call_count == 0:
            fetch_df_side_effect.call_count += 1
            return index_df
        else:
            return spy_df

    fetch_df_side_effect.call_count = 0

    mock_conn.execute.side_effect = lambda query: MagicMock(
        fetch_df=fetch_df_side_effect
    )

    performance_metrics.compute_metrics()

    mock_logger.info.assert_any_call("Metrics successfully stored in DuckDB.")
    mock_logger.info.assert_any_call("Performance metrics computation complete.")


@patch("src.performance_metrics.Path")
@patch("src.performance_metrics.Config")
@patch("src.performance_metrics.logger")
def test_compute_metrics_missing_db_file(mock_logger, mock_config, mock_path):
    mock_config.DUCKDB_FILE = "non_existent.db"
    mock_path.return_value.exists.return_value = False

    performance_metrics.compute_metrics()

    mock_logger.error.assert_called_with("DuckDB file not found at non_existent.db")


@patch("src.performance_metrics.Path")
@patch("src.performance_metrics.Config")
@patch("src.performance_metrics.logger")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_empty_index_table(
    mock_connect, mock_logger, mock_config, mock_path
):
    mock_config.DUCKDB_FILE = "mock.db"
    mock_path.return_value.exists.return_value = True

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    empty_df = pd.DataFrame()
    spy_df = pd.DataFrame({"date": [datetime.today()], "spy_value": [4000]})
    mock_conn.execute.side_effect = lambda q: MagicMock(
        fetch_df=lambda: empty_df if "index_values" in q else spy_df
    )

    performance_metrics.compute_metrics()

    found = any(
        "index_values table is empty." in str(call[0][0])
        for call in mock_logger.error.call_args_list
    )
    assert found, "Expected logger.error for empty index_values"


@patch("src.performance_metrics.Path")
@patch("src.performance_metrics.Config")
@patch("src.performance_metrics.logger")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_empty_market_index(
    mock_connect, mock_logger, mock_config, mock_path
):
    mock_config.DUCKDB_FILE = "mock.db"
    mock_path.return_value.exists.return_value = True

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    index_df = pd.DataFrame(
        {
            "date": pd.date_range(start="2024-01-01", periods=5),
            "index_value": [100, 101, 102, 103, 104],
            "tickers": ["X,Y,Z"] * 5,
        }
    )
    empty_df = pd.DataFrame()
    mock_conn.execute.side_effect = lambda q: MagicMock(
        fetch_df=lambda: index_df if "index_values" in q else empty_df
    )

    performance_metrics.compute_metrics()

    found = any(
        "market_index table is empty." in str(call[0][0])
        for call in mock_logger.error.call_args_list
    )
    assert found, "Expected logger.error for empty market_index"


@patch("src.performance_metrics.Path")
@patch("src.performance_metrics.Config")
@patch("src.performance_metrics.logger")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_rollback_on_exception(
    mock_connect, mock_logger, mock_config, mock_path
):
    mock_config.DUCKDB_FILE = "mock.db"
    mock_path.return_value.exists.return_value = True

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # This will crash during merge due to mismatched structure
    mock_conn.execute.side_effect = lambda q: MagicMock(
        fetch_df=lambda: pd.DataFrame({"wrong_column": [1]})
    )

    performance_metrics.compute_metrics()

    assert mock_conn.execute.call_args_list[0][0][0].startswith("SELECT")
    found = any(
        "Error computing metrics" in str(call[0][0])
        for call in mock_logger.error.call_args_list
    )
    assert found, "Expected logger.error for exception during processing"
