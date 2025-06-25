import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, mock_open, call
from datetime import datetime
from pathlib import Path
from src.excel_exporter import (
    safe_split,
    transform_composition,
    compute_composition_changes,
    export_to_excel,
    write_excel,
    load_data_from_duckdb,
)

# -----------------------
# Unit Tests: safe_split
# -----------------------


@pytest.mark.parametrize(
    "input_val, expected",
    [
        (["AAPL", "MSFT"], ["AAPL", "MSFT"]),
        ("['GOOG', 'TSLA']", ["GOOG", "TSLA"]),
        ("AAPL, MSFT", ["AAPL", "MSFT"]),
        ("[AAPL,MSFT]", ["AAPL", "MSFT"]),
        (None, []),
        (123, []),
    ],
)
def test_safe_split(input_val, expected):
    assert safe_split(input_val) == expected


# ----------------------------------------
# Unit Test: transform_composition
# ----------------------------------------


def test_transform_composition():
    df = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-02"],
            "tickers": ["['AAPL','MSFT']", "['AAPL','GOOG']"],
        }
    )
    result = transform_composition(df)

    assert list(result.columns) == ["date", "ticker_1", "ticker_2"]
    assert result.iloc[0]["ticker_1"] == "AAPL"
    assert result.iloc[0]["ticker_2"] == "MSFT"
    assert result.iloc[1]["ticker_2"] == "GOOG"


# ----------------------------------------
# Unit Test: compute_composition_changes
# ----------------------------------------


def test_compute_composition_changes():
    df = pd.DataFrame(
        {
            "date": ["2024-06-01", "2024-06-02"],
            "tickers": ["['AAPL','MSFT']", "['AAPL','GOOG']"],
        }
    )

    result = compute_composition_changes(df)

    assert list(result.columns) == ["date", "added", "removed", "intersection_size"]
    assert result.iloc[0]["added"] == "AAPL,MSFT"
    assert result.iloc[0]["removed"] == ""
    assert result.iloc[1]["added"] == "GOOG"
    assert result.iloc[1]["removed"] == "MSFT"
    assert result.iloc[1]["intersection_size"] == 1


# ----------------------------------------------------------
# Unit Test: load_data_from_duckdb (mocked duckdb connect)
# ----------------------------------------------------------


@patch("src.excel_exporter.duckdb.connect")
def test_load_data_from_duckdb(mock_connect):
    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.execute.return_value.fetch_df.side_effect = [
        pd.DataFrame({"date": ["2024-06-01"], "index_value": [1000]}),
        pd.DataFrame({"date": ["2024-06-01"], "tickers": ["['AAPL']"]}),
        pd.DataFrame({"date": ["2024-06-01"], "sharpe": [1.2]}),
    ]

    perf, comp, summ = load_data_from_duckdb("2024-06-01", "2024-06-10")

    assert not perf.empty
    assert not comp.empty
    assert not summ.empty
    mock_conn.close.assert_called_once()


# ----------------------------------------------------------
# Integration Test: export_to_excel (mocked end-to-end)
# ----------------------------------------------------------


@patch("src.excel_exporter.write_excel")
@patch("src.excel_exporter.load_data_from_duckdb")
@patch("src.excel_exporter.Path.mkdir")
def test_export_to_excel_success(mock_mkdir, mock_load, mock_write):
    dummy_date = "2024-06-15"
    dummy_df = pd.DataFrame({"date": ["2024-06-15"], "tickers": ["['AAPL']"]})
    mock_load.return_value = (
        pd.DataFrame({"date": ["2024-06-15"], "index_value": [1000]}),
        dummy_df,
        pd.DataFrame({"date": ["2024-06-15"], "sharpe": [1.2]}),
    )

    export_to_excel(date=dummy_date, output_dir="tests/output")

    mock_mkdir.assert_called_once()
    mock_write.assert_called_once()
    args, kwargs = mock_write.call_args
    assert isinstance(args[0], pd.DataFrame)  # performance_df
    assert isinstance(args[1], pd.DataFrame)  # transformed composition
    assert isinstance(args[2], pd.DataFrame)  # changes
    assert isinstance(args[3], pd.DataFrame)  # summary
    assert args[4].endswith("2024-06-15.xlsx")


# -----------------------------------------------
# Edge Case: export_to_excel with bad date input
# -----------------------------------------------


def test_export_to_excel_invalid_date(caplog):
    with caplog.at_level("ERROR"):
        export_to_excel(date="invalid-date", output_dir="tests/output")
        assert "Export failed" in caplog.text
