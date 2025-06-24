import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.index_builder import build_index, fetch_top_100_by_market_cap, fetch_spy_value


# -------------------------
# Fixtures and Helpers
# -------------------------

@pytest.fixture
def mock_conn():
    return MagicMock()


@pytest.fixture
def sample_top_100_df():
    return pd.DataFrame({
        'ticker': [f'TICK{i}' for i in range(100)],
        'close': [100 + i for i in range(100)]
    })


@pytest.fixture
def sample_dates_df():
    return pd.DataFrame({
        'date': ['2024-01-01', '2024-01-02']
    })


# -------------------------
# Unit Tests for Helper Functions
# -------------------------

def test_fetch_top_100_by_market_cap_success(mock_conn, sample_top_100_df):
    mock_conn.execute.return_value.fetch_df.return_value = sample_top_100_df
    df = fetch_top_100_by_market_cap(mock_conn, '2024-01-01')
    assert df is not None
    assert len(df) == 100


def test_fetch_top_100_by_market_cap_insufficient(mock_conn):
    mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame({'ticker': ['A'], 'close': [100]})
    df = fetch_top_100_by_market_cap(mock_conn, '2024-01-01')
    assert df is None


def test_fetch_top_100_by_market_cap_exception(mock_conn):
    mock_conn.execute.side_effect = Exception("DB error")
    df = fetch_top_100_by_market_cap(mock_conn, '2024-01-01')
    assert df is None


def test_fetch_spy_value_success(mock_conn):
    mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame({'spy_close': [432.123456]})
    val = fetch_spy_value(mock_conn, '2024-01-01')
    assert val == 432.1235


def test_fetch_spy_value_empty(mock_conn):
    mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame()
    val = fetch_spy_value(mock_conn, '2024-01-01')
    assert val is None


def test_fetch_spy_value_exception(mock_conn):
    mock_conn.execute.side_effect = Exception("DB error")
    val = fetch_spy_value(mock_conn, '2024-01-01')
    assert val is None


# -------------------------
# Integration-style Tests for build_index
# -------------------------

@patch("src.index_builder.fetch_top_100_by_market_cap")
@patch("src.index_builder.fetch_spy_value")
@patch("src.index_builder.Path.exists")
@patch("src.index_builder.duckdb.connect")
def test_build_index_happy_path(mock_connect, mock_exists, mock_fetch_spy, mock_fetch_top):
    mock_exists.return_value = True
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    # Simulate execute calls: stock_prices, dates, BEGIN, CREATE TABLE, INSERT, COMMIT
    mock_conn.execute.side_effect = [
        MagicMock(fetch_df=MagicMock(return_value=pd.DataFrame({'dummy': [1]}))),  # stock_prices
        MagicMock(fetch_df=MagicMock(return_value=pd.DataFrame({'date': ['2024-01-01']}))),  # dates
        None,  # BEGIN
        None,  # CREATE TABLE
        None,  # INSERT
        None,  # COMMIT
    ]

    mock_fetch_top.return_value = pd.DataFrame({
        'ticker': [f'T{i}' for i in range(100)],
        'close': [1.0 for _ in range(100)]
    })
    mock_fetch_spy.return_value = 500.0

    build_index()

    assert mock_fetch_top.called
    assert mock_fetch_spy.called
    assert mock_conn.execute.call_count >= 6


@patch("src.index_builder.logger")
@patch("src.index_builder.Path.exists", return_value=False)
def test_build_index_duckdb_missing(_, mock_log):
    from src.config import Config
    build_index()
    mock_log.error.assert_called_once_with(f"DuckDB file not found: {Config.DUCKDB_FILE}")


@patch("src.index_builder.logger")
@patch("src.index_builder.duckdb.connect")
@patch("src.index_builder.Path.exists", return_value=True)
def test_build_index_empty_stock_data(_, mock_connect, mock_log):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame()

    build_index()

    mock_log.warning.assert_any_call("No data in `stock_prices`. Aborting index build.")


@patch("src.index_builder.logger")
@patch("src.index_builder.fetch_top_100_by_market_cap", return_value=None)
@patch("src.index_builder.duckdb.connect")
@patch("src.index_builder.Path.exists", return_value=True)
def test_build_index_no_valid_top_100(_, mock_connect, mock_fetch_top, mock_log):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    mock_conn.execute.side_effect = [
        MagicMock(fetch_df=MagicMock(return_value=pd.DataFrame({'ticker': ['A'], 'close': [100]}))),  # stock_prices
        MagicMock(fetch_df=MagicMock(return_value=pd.DataFrame({'date': ['2024-01-01']}))),  # dates
    ]

    build_index()
    mock_log.error.assert_called_with("No index values calculated. Aborting.")
