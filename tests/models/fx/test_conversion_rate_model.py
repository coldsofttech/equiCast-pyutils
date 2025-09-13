import json
import os
import tempfile

import pandas as pd
import pytest

from equicast_pyutils.models.fx.conversion_rate_model import FxConversionRateModel


@pytest.fixture
def fx_model():
    model = FxConversionRateModel(pair="USDGBP", from_currency="USD", to_currency="GBP")
    model.add_rate("2025-01-01", 0.78)
    model.add_rate("2025-01-02", 0.79)
    return model


def test_is_empty_property():
    model = FxConversionRateModel(pair="USDGBP", from_currency="USD", to_currency="GBP")
    assert model.is_empty is True
    model.add_rate("2025-01-01", 0.78)
    assert model.is_empty is False


def test_to_json_string(fx_model):
    json_str = fx_model.to_json()
    data = json.loads(json_str)

    assert data["pair"] == "USDGBP"
    assert "2025-01-01" in data["rates"]
    assert data["rates"]["2025-01-01"] == 0.78
    assert "lastUpdated" in data["metadata"]


def test_to_json_file(fx_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "fx.json")
        fx_model.to_json(filepath)

        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["rates"]["2025-01-02"] == 0.79


def test_to_parquet_file(fx_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "fx.parquet")
        fx_model.to_parquet(filepath)

        df = pd.read_parquet(filepath)

        assert set(["pair", "from", "to", "date", "rate"]).issubset(df.columns)
        row = df[df["date"] == "2025-01-01"].iloc[0]
        assert row["pair"] == "USDGBP"
        assert row["rate"] == 0.78


def test_to_parquet_file_empty_model():
    model = FxConversionRateModel(pair="USDGBP", from_currency="USD", to_currency="GBP")
    assert model.is_empty

    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "fx.parquet")
        model.to_parquet(filepath)

        assert not os.path.exists(filepath)


def test_add_rate_valid_date_updates_metadata(fx_model):
    before_update = fx_model.metadata["lastUpdated"]
    fx_model.add_rate("2025-01-03", 0.80)
    assert fx_model.rates["2025-01-03"] == 0.80
    assert "lastUpdated" in fx_model.metadata
    assert fx_model.metadata["lastUpdated"] != before_update


def test_add_rate_valid_date(fx_model):
    fx_model.add_rate("2025-01-03", 0.80)
    assert fx_model.rates["2025-01-03"] == 0.80
    assert "lastUpdated" in fx_model.metadata


def test_add_rate_invalid_date(fx_model):
    with pytest.raises(ValueError, match="Date must be in YYYY-MM-DD format"):
        fx_model.add_rate("01-2025-01", 0.85)


def test_to_dataframe_structure(fx_model):
    df = fx_model._to_dataframe()
    assert list(df.columns) == ["pair", "from", "to", "date", "rate"]
    assert len(df) == len(fx_model.rates)
    assert 0.79 in df["rate"].values
