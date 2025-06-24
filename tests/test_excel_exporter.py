import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.excel_exporter import (
    safe_split,
    transform_composition,
    compute_composition_changes,
    export_to_excel,
)
from src.config import Config


# -----------------------
# Unit Tests: safe_split
# -----------------------


@pytest.mark.parametrize(
    "input_val, expected",
    [
        (["AAPL", "MSFT"], ["AAPL", "MSFT"]),
        ("['AAPL','MSFT']", ["AAPL", "MSFT"]),
        ("AAPL,MSFT,GOOG", ["AAPL", "MSFT", "GOOG"]),
        ("[]", []),
        ("", []),
        (None, []),
    ],
)
def test_safe_split_various_inputs(input_val, expected):
    assert safe_split(input_val) == expected


# -------------------------------
# Unit Test: transform_composition
# -------------------------------


def test_transform_composition():
    df = pd.DataFrame(
        {"date": ["2024-01-01", "2024-01-02"], "tickers": ["AAPL,MSFT", "GOOG,TSLA"]}
    )
    result = transform_composition(df)
    assert "ticker_1" in result.columns
    assert result.shape == (2, 3)
    assert result.iloc[0]["ticker_1"] == "AAPL"


# -----------------------------------------
# Unit Test: compute_composition_changes
# -----------------------------------------


def test_compute_composition_changes():
    df = pd.DataFrame(
        {"date": ["2024-01-01", "2024-01-02"], "tickers": ["AAPL,MSFT", "AAPL,GOOG"]}
    )
    result = compute_composition_changes(df)
    assert result.shape[0] == 2
    assert "added" in result.columns
    assert result.iloc[1]["added"] == "GOOG"
    assert result.iloc[1]["removed"] == "MSFT"
    assert result.iloc[1]["intersection_size"] == 1


# --------------------------------------------------
# Integration-style test: export_to_excel (patched)
# --------------------------------------------------


@patch.object(Config, "EXCEL_OUTPUT_FILE", "dummy.xlsx")
@patch.object(Config, "DUCKDB_FILE", "dummy.db")
@patch("src.excel_exporter.logger")
@patch("src.excel_exporter.os.remove")
@patch("src.excel_exporter.Path.exists", return_value=True)
@patch("src.excel_exporter.write_excel")
@patch("src.excel_exporter.load_data_from_duckdb")
def test_export_to_excel_success(
    mock_load_data,
    mock_write_excel,
    mock_exists,
    mock_remove,
    mock_logger,
    *_  # *_ for config patches
):
    perf = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "index_value": [1000],
            "daily_return": [0.01],
            "cumulative_return": [0.01],
        }
    )
    comp = pd.DataFrame({"date": ["2024-01-01"], "tickers": ["AAPL,MSFT"]})
    summ = pd.DataFrame({"metric": ["max_return"], "value": [0.05]})

    mock_load_data.return_value = (perf, comp, summ)

    export_to_excel()

    assert mock_remove.called
    assert mock_write_excel.called
    assert "Excel export successful" in mock_logger.info.call_args_list[-1][0][0]


@patch.object(Config, "DUCKDB_FILE", "missing.db")
@patch("src.excel_exporter.logger")
@patch("src.excel_exporter.Path.exists", return_value=False)
def test_export_to_excel_missing_duckdb(mock_exists, mock_logger, *_):
    export_to_excel()
    assert mock_logger.error.called
    assert "DuckDB file not found" in mock_logger.error.call_args[0][0]


@patch.object(Config, "DUCKDB_FILE", "dummy.db")
@patch("src.excel_exporter.logger")
@patch("src.excel_exporter.Path.exists", return_value=True)
@patch("src.excel_exporter.load_data_from_duckdb", side_effect=Exception("fail"))
def test_export_to_excel_failure(mock_load_data, mock_exists, mock_logger, *_):
    export_to_excel()
    error_calls = [call[0][0] for call in mock_logger.error.call_args_list]
    assert any("Export failed" in msg for msg in error_calls)
