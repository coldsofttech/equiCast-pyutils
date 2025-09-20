import json

import pandas as pd
import pytest

from equicast_pyutils.models.stock import FundamentalsModel, OHLCModel


@pytest.fixture
def sample_model():
    model = FundamentalsModel(ticker="AAPL")
    model.day = OHLCModel(low=147.0, high=151.0, open=148.0, close=151.0)
    model.one_year = OHLCModel(low=120.0, high=180.0, open=120.0, close=180.0)
    return model


def test_initialization(sample_model):
    assert sample_model.ticker == "AAPL"
    assert sample_model.day.close == 151.0
    assert sample_model.day.open == 148.0
    assert sample_model.day.low == 147.0
    assert sample_model.day.high == 151.0


def test_is_empty():
    empty_model = FundamentalsModel(ticker="TSLA")
    assert empty_model.is_empty is True

    non_empty_model = FundamentalsModel(ticker="TSLA")
    non_empty_model.currency = "USD"
    assert non_empty_model.is_empty is False


def test_to_json_string(sample_model):
    json_str = sample_model.to_json()
    data = json.loads(json_str)
    assert data["ticker"] == "AAPL"
    assert data["day"]["close"] == 151.0
    assert "metadata" in data


def test_to_json_file(tmp_path, sample_model):
    filepath = tmp_path / "fundamentals.json"
    sample_model.to_json(filepath=str(filepath))
    assert filepath.exists()

    with open(filepath, "r") as f:
        data = json.load(f)
    assert data["ticker"] == "AAPL"


def test_to_dataframe(sample_model):
    df = sample_model._to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["day_low"] == 147.0
    assert df.iloc[0]["day_high"] == 151.0
    assert df.iloc[0]["one_year_low"] == 120.0


def test_to_parquet(tmp_path, sample_model):
    filepath = tmp_path / "fundamentals.parquet"
    sample_model.to_parquet(str(filepath))
    assert filepath.exists()

    df = pd.read_parquet(filepath)
    assert df.iloc[0]["ticker"] == "AAPL"
    assert df.iloc[0]["day_low"] == 147.0


def test_flatten_dataclass_handles_empty_fields():
    model = FundamentalsModel(ticker="MSFT")
    df = model._to_dataframe()
    columns = df.columns.tolist()
    assert "ticker" in columns
    assert "metadata" in columns
    for col in ["close", "open", "day_low", "one_year_low"]:
        assert col not in columns


def test_export_import_consistency(tmp_path, sample_model):
    json_path = tmp_path / "fund.json"
    sample_model.to_json(filepath=str(json_path))
    with open(json_path, "r") as f:
        loaded_data = json.load(f)
    assert loaded_data["ticker"] == sample_model.ticker
    assert "metadata" in loaded_data

    parquet_path = tmp_path / "fund.parquet"
    sample_model.to_parquet(str(parquet_path))
    df = pd.read_parquet(parquet_path)
    assert df.iloc[0]["ticker"] == sample_model.ticker
