import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd

import src.data_ingestion as ingestion


class TestDataIngestion(unittest.TestCase):

    @patch("src.data_ingestion.requests.Session.get")
    def test_get_finnhub_tickers_success(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [
            {
                "symbol": "AAPL",
                "type": "Common Stock",
                "isEnabled": True,
                "status": "active",
            },
            {
                "symbol": "TSLA",
                "type": "Common Stock",
                "isEnabled": True,
                "status": "active",
            },
        ]

        with patch("src.data_ingestion.Config.FINNHUB_API_KEY", "dummy_key"):
            tickers = ingestion.get_finnhub_tickers()
            self.assertIn("AAPL", tickers)
            self.assertIn("TSLA", tickers)

    @patch("src.data_ingestion.requests.Session.get")
    def test_get_finnhub_tickers_fail(self, mock_get):
        mock_get.side_effect = Exception("API failed")
        with patch("src.data_ingestion.Config.FINNHUB_API_KEY", "dummy_key"):
            tickers = ingestion.get_finnhub_tickers()
            self.assertEqual(tickers, [])

    @patch("src.data_ingestion.requests.Session.get")
    def test_get_sp500_tickers_success(self, mock_get):
        html = """
        <table id="constituents">
            <tr><th>Symbol</th></tr>
            <tr><td>AAPL</td></tr>
            <tr><td>GOOG</td></tr>
        </table>
        """
        mock_get.return_value.text = html
        tickers = ingestion.get_sp500_tickers()
        self.assertEqual(tickers, ["AAPL", "GOOG"])

    @patch("src.data_ingestion.requests.Session.get")
    def test_get_sp500_tickers_fail(self, mock_get):
        mock_get.side_effect = Exception("wiki fail")
        tickers = ingestion.get_sp500_tickers()
        self.assertEqual(tickers, [])

    @patch("src.data_ingestion.yf.Ticker")
    def test_fetch_and_prepare_stock_data_success(self, mock_ticker_class):
        mock_ticker = MagicMock()
        df = pd.DataFrame(
            {
                "Date": [pd.Timestamp("2024-06-24")],
                "Close": [100.0],
            }
        )
        mock_ticker.history.return_value = df
        mock_ticker.info = {"sharesOutstanding": 1000000}
        mock_ticker_class.return_value = mock_ticker

        result = ingestion.fetch_and_prepare_stock_data("AAPL", "2024-06-24")
        self.assertIsNotNone(result)
        self.assertEqual(result.iloc[0]["ticker"], "AAPL")
        self.assertGreater(result.iloc[0]["market_cap"], 0)

    @patch("src.data_ingestion.yf.Ticker")
    def test_fetch_and_prepare_stock_data_fail(self, mock_ticker_class):
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker.info = {"sharesOutstanding": 1000000}
        mock_ticker_class.return_value = mock_ticker

        result = ingestion.fetch_and_prepare_stock_data("AAPL", "2024-06-24")
        self.assertIsNone(result)

    @patch("src.data_ingestion.yf.Ticker")
    def test_fetch_spy_data_success(self, mock_ticker_class):
        df = pd.DataFrame({"Date": [pd.Timestamp("2024-06-24")], "Close": [5000.0]})

        mock_ticker = MagicMock()
        mock_ticker.history.return_value = df
        mock_ticker_class.return_value = mock_ticker

        conn = MagicMock()
        ingestion.fetch_spy_data(conn, "2024-06-24")
        conn.execute.assert_called()
        conn.register.assert_called()
        conn.unregister.assert_called()

    def test_create_tables(self):
        conn = MagicMock()
        ingestion.create_tables(conn)
        self.assertTrue(conn.execute.called)

    def test_create_requests_session(self):
        session = ingestion.create_requests_session()
        self.assertIsInstance(session, ingestion.requests.Session)

    @patch("src.data_ingestion.get_finnhub_tickers", return_value=["AAPL", "GOOG"])
    @patch("src.data_ingestion.fetch_all_stocks_parallel")
    @patch("src.data_ingestion.fetch_spy_data")
    @patch("src.data_ingestion.create_tables")
    @patch("src.data_ingestion.duckdb.connect")
    def test_run_ingestion_success(
        self,
        mock_connect,
        mock_create_tables,
        mock_fetch_spy,
        mock_fetch_stocks,
        mock_get_tickers,
    ):
        conn_mock = MagicMock()
        mock_connect.return_value = conn_mock

        with patch("src.data_ingestion.Config.FINNHUB_API_KEY", "dummy"), patch(
            "src.data_ingestion.datetime"
        ) as mock_dt:
            mock_dt.strptime.return_value = datetime(2024, 6, 24)
            ingestion.run_ingestion("2024-06-24")
            mock_create_tables.assert_called_once()
            mock_fetch_stocks.assert_called_once()
            mock_fetch_spy.assert_called_once()

    @patch("src.data_ingestion.get_finnhub_tickers", return_value=[])
    @patch("src.data_ingestion.get_sp500_tickers", return_value=[])
    @patch("src.data_ingestion.duckdb.connect")
    def test_run_ingestion_no_tickers(self, mock_connect, mock_sp500, mock_finnhub):
        with patch("src.data_ingestion.Config.FINNHUB_API_KEY", "dummy"), patch(
            "src.data_ingestion.datetime"
        ) as mock_dt:
            mock_dt.strptime.return_value = datetime(2024, 6, 24)
            ingestion.run_ingestion("2024-06-24")
            self.assertTrue(mock_sp500.called)


if __name__ == "__main__":
    unittest.main()
