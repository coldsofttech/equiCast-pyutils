import json
import os
from datetime import datetime

import pandas as pd
import pytest

from equicast_pyutils.models.stock import CompanyProfileModel, CompanyAddressModel, CompanyOfficerModel


@pytest.fixture
def sample_model():
    officer_model = CompanyOfficerModel()
    officer_model.name = "Tim Cook"
    officer_model.title = "CEO"
    model = CompanyProfileModel(ticker="AAPL")
    model.exchange = "NASDAQ"
    model.currency = "USD"
    model.name = "Apple Inc."
    model.address = CompanyAddressModel()
    model.address.city = "Cupertino"
    model.address.country = "USA"
    model.ceos = [officer_model]
    model.metadata = {"lastUpdated": datetime(2025, 1, 1).isoformat()}
    model.ipo_date = datetime(1980, 12, 12)
    return model


def test_is_empty(sample_model):
    assert not sample_model.is_empty
    empty_model = CompanyProfileModel(ticker="TEST")
    assert empty_model.is_empty


def test_to_json_string(sample_model):
    json_str = sample_model.to_json()
    data = json.loads(json_str)
    assert data["ticker"] == "AAPL"
    assert data["ceos"][0]["name"] == "Tim Cook"
    assert data["address"]["city"] == "Cupertino"


def test_to_json_file(tmp_path, sample_model):
    file_path = tmp_path / "profile.json"
    sample_model.to_json(filepath=file_path)
    assert os.path.exists(file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["ticker"] == "AAPL"


def test_to_parquet(tmp_path, sample_model):
    file_path = tmp_path / "profile.parquet"
    sample_model.to_parquet(filepath=file_path)
    assert os.path.exists(file_path)
    df = pd.read_parquet(file_path)
    assert "ticker" in df.columns
    assert "ceos" in df.columns
    assert "address_city" in df.columns
    ceos_list = json.loads(df["ceos"].iloc[0])
    assert ceos_list[0]["name"] == "Tim Cook"
    metadata = json.loads(df["metadata"].iloc[0])
    assert metadata["lastUpdated"] == "2025-01-01T00:00:00"


def test_to_parquet_empty_model(tmp_path):
    empty_model = CompanyProfileModel(ticker="EMPTY")
    file_path = tmp_path / "empty.parquet"
    empty_model.to_parquet(filepath=file_path)
    df = pd.read_parquet(file_path)
    assert df["ticker"].iloc[0] == "EMPTY"


def test_dataframe_structure(sample_model):
    df = sample_model._to_dataframe()
    assert "ticker" in df.columns
    assert "ceos" in df.columns
    assert "address_city" in df.columns
    assert "metadata" in df.columns
    if "ipo_date" in df.columns:
        val = df["ipo_date"].iloc[0]
        assert val is None or isinstance(val, (str, pd.Timestamp, datetime))


def test_address_fields(sample_model):
    df = sample_model._to_dataframe()
    assert df["address_city"].iloc[0] == "Cupertino"
    assert df["address_country"].iloc[0] == "USA"


def test_ceos_json_format(sample_model):
    df = sample_model._to_dataframe()
    ceos_json = df["ceos"].iloc[0]
    ceos_list = json.loads(ceos_json)
    assert isinstance(ceos_list, list)
    assert ceos_list[0]["name"] == "Tim Cook"


def test_metadata_json_format(sample_model):
    df = sample_model._to_dataframe()
    metadata_json = df["metadata"].iloc[0]
    metadata_dict = json.loads(metadata_json)
    assert metadata_dict["lastUpdated"] == "2025-01-01T00:00:00"


def test_ipo_date_field(sample_model):
    df = sample_model._to_dataframe()
    if "ipo_date" in df.columns:
        ipo_str = df["ipo_date"].iloc[0]
        dt = datetime.fromisoformat(ipo_str)
        assert dt.year == 1980
