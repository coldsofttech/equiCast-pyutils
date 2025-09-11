import json
import os
import tempfile

import pandas as pd
import pytest

from equicast_pyutils.models.stock import StockPriceModel


@pytest.fixture
def stock_price_model():
    model = StockPriceModel(ticker="AAPL")
    model.add_price("2025-01-01", 172.56)
    model.add_price("2025-01-02", 173.48)
    model.currency = "USD"
    return model


def test_to_json_string(stock_price_model):
    json_str = stock_price_model.to_json()
    data = json.loads(json_str)

    assert data["ticker"] == "AAPL"
    assert data["currency"] == "USD"
    assert "2025-01-01" in data["prices"]
    assert data["prices"]["2025-01-01"] == 172.56
    assert "lastUpdated" in data["metadata"]


def test_to_json_file(stock_price_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "stock_price.json")
        stock_price_model.to_json(filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["prices"]["2025-01-02"] == 173.48


def test_to_parquet_file(stock_price_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "stock_price.parquet")
        stock_price_model.to_parquet(filepath)

        df = pd.read_parquet(filepath)

        assert set(["ticker", "currency", "date", "price"]).issubset(df.columns)
        row = df[df["date"] == "2025-01-01"].iloc[0]
        assert row["ticker"] == "AAPL"
        assert row["currency"] == "USD"
        assert row["price"] == 172.56


def test_add_price_valid_date(stock_price_model):
    stock_price_model.add_price("2025-01-03", 174.58)
    assert stock_price_model.prices["2025-01-03"] == 174.58
    assert "lastUpdated" in stock_price_model.metadata


def test_add_price_invalid_date(stock_price_model):
    with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
        stock_price_model.add_price("01-2025-01", 174.59)


def test_to_dataframe_structure(stock_price_model):
    df = stock_price_model._to_dataframe()
    assert list(df.columns) == ["ticker", "currency", "date", "price"]
    assert len(df) == len(stock_price_model.prices)
    assert 172.56 in df["price"].values
