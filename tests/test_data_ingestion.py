import pytest
import pandas as pd
import duckdb
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from data_ingestion import (
    fetch_and_prepare_stock_data,
    get_finnhub_tickers,
    get_sp500_tickers,
    create_tables,
    fetch_all_stocks_parallel,
    fetch_spy_data
)

# --- Fixtures ---
@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(database=':memory:')
    create_tables(conn)
    return conn

# --- Tests ---
def test_create_tables(duckdb_conn):
    tables = duckdb_conn.execute("SHOW TABLES").fetchall()
    assert ('stock_prices',) in tables
    assert ('market_index',) in tables

def test_fetch_and_prepare_stock_data_valid():
    # Patch yfinance.Ticker
    with patch('ingestion.yf.Ticker') as mock_ticker:
        mock_stock = MagicMock()
        mock_stock.history.return_value = pd.DataFrame({
            'Date': pd.date_range(end=datetime.today(), periods=3),
            'Close': [150, 152, 154]
        })
        mock_stock.info = {'sharesOutstanding': 1000000}
        mock_ticker.return_value = mock_stock

        df = fetch_and_prepare_stock_data('AAPL', '2024-01-01', '2024-01-10')
        assert df is not None
        assert set(['date', 'ticker', 'close', 'market_cap']).issubset(df.columns)

def test_fetch_and_prepare_stock_data_invalid():
    with patch('ingestion.yf.Ticker') as mock_ticker:
        mock_stock = MagicMock()
        mock_stock.history.return_value = pd.DataFrame()
        mock_stock.info = {'sharesOutstanding': 1000000}
        mock_ticker.return_value = mock_stock

        df = fetch_and_prepare_stock_data('INVALID', '2024-01-01', '2024-01-10')
        assert df is None

def test_get_finnhub_tickers_no_key():
    with patch('ingestion.Config.FINNHUB_API_KEY', None):
        result = get_finnhub_tickers()
        assert result == []

def test_get_sp500_tickers():
    html_mock = '''<table id="constituents"><tr><th>Symbol</th></tr>
                   <tr><td>AAPL</td></tr><tr><td>MSFT</td></tr></table>'''
    with patch('ingestion.session.get') as mock_get:
        mock_resp = MagicMock()
        mock_resp.text = html_mock
        mock_get.return_value = mock_resp
        tickers = get_sp500_tickers()
        assert tickers == ['AAPL', 'MSFT']

def test_fetch_all_stocks_parallel(duckdb_conn):
    with patch('ingestion.fetch_and_prepare_stock_data') as mock_fetch:
        df = pd.DataFrame({
            'date': pd.date_range(end=datetime.today(), periods=3),
            'ticker': ['FAKE'] * 3,
            'close': [100, 101, 102],
            'market_cap': [1e9, 1.01e9, 1.02e9]
        })
        mock_fetch.return_value = df

        fetch_all_stocks_parallel(['FAKE'], duckdb_conn, '2024-01-01', '2024-01-10', max_workers=2)
        rows = duckdb_conn.execute("SELECT * FROM stock_prices").df()
        assert len(rows) == 3

def test_fetch_spy_data(duckdb_conn):
    with patch('ingestion.yf.Ticker') as mock_ticker:
        mock_stock = MagicMock()
        mock_stock.history.return_value = pd.DataFrame({
            'Date': pd.date_range(end=datetime.today(), periods=2),
            'Close': [4000, 4100]
        })
        mock_ticker.return_value = mock_stock

        fetch_spy_data(duckdb_conn, '2024-01-01', '2024-01-10')
        rows = duckdb_conn.execute("SELECT * FROM market_index").df()
        assert len(rows) == 2