import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.summary_metrics_calculator import compute_summary_metrics
from src.config import Config


@pytest.fixture
def dummy_index_metrics():
    return pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "daily_return": [0.01, -0.005],
            "spy_return": [0.02, -0.01],
            "drawdown": [-0.01, -0.02],
            "drawdown_pct": [-1.0, -2.0],
            "turnover": [1.0, 0.0],
            "exposure_similarity": [0.95, 0.90],
        }
    )


def make_fake_duckdb(mock_df):
    mock_conn = MagicMock()

    def execute_side_effect(query):
        if "SELECT * FROM index_metrics" in query:
            return MagicMock(fetch_df=lambda: mock_df)
        return MagicMock()

    mock_conn.execute.side_effect = execute_side_effect
    mock_conn.register = MagicMock()
    mock_conn.unregister = MagicMock()
    return mock_conn


@patch("src.summary_metrics_calculator.Path.exists", return_value=True)
@patch("src.summary_metrics_calculator.duckdb.connect")
@patch.object(Config, "get_fetch_days", return_value=2)
def test_compute_summary_metrics_with_data(_, mock_connect, __, dummy_index_metrics):
    fake_conn = make_fake_duckdb(dummy_index_metrics)
    mock_connect.return_value = fake_conn

    compute_summary_metrics("2024-01-02")

    insert_calls = [
        call
        for call in fake_conn.execute.call_args_list
        if "INSERT INTO summary_metrics" in str(call)
    ]
    assert insert_calls  # should insert with real data


@patch("src.summary_metrics_calculator.Path.exists", return_value=True)
@patch("src.summary_metrics_calculator.duckdb.connect")
@patch.object(Config, "get_fetch_days", return_value=2)
def test_compute_summary_metrics_with_empty_data(_, mock_connect, __):
    fake_conn = make_fake_duckdb(pd.DataFrame())
    mock_connect.return_value = fake_conn

    compute_summary_metrics("2024-01-02")

    insert_calls = [
        call
        for call in fake_conn.execute.call_args_list
        if "INSERT INTO summary_metrics" in str(call)
    ]
    assert insert_calls  # should still insert null summary
