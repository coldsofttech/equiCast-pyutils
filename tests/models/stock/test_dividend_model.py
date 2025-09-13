import json
import os
import tempfile

import pandas as pd
import pytest

from equicast_pyutils.models.stock import DividendModel


@pytest.fixture
def dividend_model():
    model = DividendModel(ticker="AAPL")
    model.add_price("2025-01-01", 172.56)
    model.add_price("2025-01-02", 173.48)
    model.currency = "USD"
    return model


def test_is_empty_property():
    empty_model = DividendModel(ticker="MSFT", currency="USD")
    assert empty_model.is_empty is True

    empty_model.add_price("2025-01-01", 100.0)
    assert empty_model.is_empty is False


def test_to_json_string(dividend_model):
    json_str = dividend_model.to_json()
    data = json.loads(json_str)

    assert data["ticker"] == "AAPL"
    assert data["currency"] == "USD"
    assert "2025-01-01" in data["prices"]
    assert data["prices"]["2025-01-01"] == 172.56
    assert "lastUpdated" in data["metadata"]


def test_to_json_file(dividend_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "dividends.json")
        dividend_model.to_json(filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["prices"]["2025-01-02"] == 173.48


def test_to_parquet_file(dividend_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "dividends.parquet")
        dividend_model.to_parquet(filepath)

        df = pd.read_parquet(filepath)

        assert set(["ticker", "currency", "date", "price"]).issubset(df.columns)
        row = df[df["date"] == "2025-01-01"].iloc[0]
        assert row["ticker"] == "AAPL"
        assert row["currency"] == "USD"
        assert row["price"] == 172.56


def test_to_parquet_empty_model():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "empty.parquet")
        empty_model = DividendModel(ticker="TSLA", currency="USD")
        empty_model.to_parquet(filepath)

        assert not os.path.exists(filepath)


def test_add_price_valid_date(dividend_model):
    dividend_model.add_price("2025-01-03", 174.58)
    assert dividend_model.prices["2025-01-03"] == 174.58
    assert "lastUpdated" in dividend_model.metadata


def test_add_price_invalid_date(dividend_model):
    with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
        dividend_model.add_price("01-2025-01", 174.59)


def test_to_dataframe_structure(dividend_model):
    df = dividend_model._to_dataframe()
    assert list(df.columns) == ["ticker", "currency", "date", "price"]
    assert len(df) == len(dividend_model.prices)
    assert 172.56 in df["price"].values
