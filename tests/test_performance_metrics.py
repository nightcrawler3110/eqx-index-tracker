import pandas as pd
import numpy as np
import pytest
from unittest.mock import patch, MagicMock

from src.performance_metrics import max_consecutive_streak, compute_metrics


# ---------------------------
# Unit Tests: max_consecutive_streak
# ---------------------------

@pytest.mark.parametrize("series,positive,expected", [
    ([1, 2, -1, 3, 4], True, 2),
    ([-1, -2, 0, -3, -4], False, 2),
    ([0, 0, 0], True, 0),
    ([1, 1, 1], True, 3),
    ([-1, -1, -1], False, 3),
])
def test_max_consecutive_streak(series, positive, expected):
    s = pd.Series(series)
    assert max_consecutive_streak(s, positive) == expected


# ---------------------------
# Integration-style test: compute_metrics
# ---------------------------

@patch("src.performance_metrics.logging")
@patch("src.performance_metrics.Path.exists", return_value=True)
@patch("src.performance_metrics.Config.DUCKDB_FILE", "dummy.db")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_success(mock_connect, _, mock_log):
    # Mock connection + execute
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Create dummy data
    index_df = pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=10),
        'index_value': np.linspace(100, 110, 10),
        'tickers': ['AAPL,MSFT'] * 10
    })
    spy_df = pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=10),
        'spy_close': np.linspace(400, 420, 10)
    })

    mock_conn.execute.side_effect = [
        MagicMock(fetch_df=lambda: index_df),
        MagicMock(fetch_df=lambda: spy_df),
        None,  # BEGIN
        None,  # CREATE TABLE
        None,  # CREATE INDEX
        None,  # DELETE
        None,  # INSERT
        None,  # UNREGISTER
        None,  # REGISTER summary_df
        None,  # CREATE OR REPLACE
        None,  # UNREGISTER
        None   # COMMIT
    ]

    compute_metrics()
    assert mock_log.info.call_args_list[-1][0][0] == "performance metrics computation complete."


@patch("src.performance_metrics.logging")
@patch("src.performance_metrics.Path.exists", return_value=True)
@patch("src.performance_metrics.Config.DUCKDB_FILE", "dummy.db")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_index_table_empty(mock_connect, _, mock_log):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.execute.side_effect = [
        MagicMock(fetch_df=lambda: pd.DataFrame()),  # index_df is empty
    ]

    compute_metrics()
    assert "index_values table is empty." in str(mock_log.error.call_args_list)


@patch("src.performance_metrics.logging")
@patch("src.performance_metrics.Path.exists", return_value=True)
@patch("src.performance_metrics.Config.DUCKDB_FILE", "dummy.db")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_market_index_empty(mock_connect, _, mock_log):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    index_df = pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=5),
        'index_value': np.linspace(100, 105, 5),
        'tickers': ['AAPL,MSFT'] * 5
    })

    mock_conn.execute.side_effect = [
        MagicMock(fetch_df=lambda: index_df),           # index_df
        MagicMock(fetch_df=lambda: pd.DataFrame()),     # spy_df empty
    ]

    compute_metrics()
    assert "market_index table is empty." in str(mock_log.error.call_args_list)


@patch("src.performance_metrics.logging")
@patch("src.performance_metrics.Path.exists", return_value=False)
def test_compute_metrics_duckdb_missing(mock_log, _):
    compute_metrics()
    assert mock_log.error.call_args_list[0][0][0].startswith("DuckDB file not found")


@patch("src.performance_metrics.logging")
@patch("src.performance_metrics.Path.exists", return_value=True)
@patch("src.performance_metrics.Config.DUCKDB_FILE", "dummy.db")
@patch("src.performance_metrics.duckdb.connect")
def test_compute_metrics_rolls_back_on_exception(mock_connect, _, mock_log):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    index_df = pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=5),
        'index_value': np.linspace(100, 105, 5),
        'tickers': ['AAPL,MSFT'] * 5
    })
    spy_df = pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', periods=5),
        'spy_close': np.linspace(400, 405, 5)
    })

    mock_conn.execute.side_effect = [
        MagicMock(fetch_df=lambda: index_df),  # index_df
        MagicMock(fetch_df=lambda: spy_df),    # spy_df
        Exception("Kaboom!")                   # Boom during merge/compute
    ]

    compute_metrics()
    assert any("Error computing metrics" in str(c[0][0]) for c in mock_log.error.call_args_list)
    assert mock_conn.execute.call_args_list[-1][0][0] == "ROLLBACK"

