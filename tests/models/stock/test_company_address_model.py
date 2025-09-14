import json
from dataclasses import asdict

import pandas as pd
import pytest

from equicast_pyutils.models.stock import CompanyAddressModel


@pytest.fixture
def filled_address():
    model = CompanyAddressModel()
    model.address1 = "123 Main St"
    model.address2 = "Suite 100"
    model.city = "Metropolis"
    model.state = "NY"
    model.zip = "12345"
    model.country = "USA"
    model.region = "US"
    return model


@pytest.fixture
def empty_address():
    return CompanyAddressModel()


def test_is_empty(filled_address, empty_address):
    assert not filled_address.is_empty
    assert empty_address.is_empty


def test_to_json_string(filled_address):
    json_str = filled_address.to_json()
    data = json.loads(json_str)
    for k, v in asdict(filled_address).items():
        assert data[k] == v


def test_to_json_file(tmp_path, filled_address):
    file_path = tmp_path / "address.json"
    filled_address.to_json(filepath=file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    for k, v in asdict(filled_address).items():
        assert data[k] == v


def test_to_parquet_file(tmp_path, filled_address):
    file_path = tmp_path / "address.parquet"
    filled_address.to_parquet(filepath=file_path)
    df = pd.read_parquet(file_path)
    for k, v in asdict(filled_address).items():
        assert df.loc[0, k] == v


def test_to_parquet_empty_model(tmp_path, empty_address):
    file_path = tmp_path / "empty.parquet"
    empty_address.to_parquet(filepath=file_path)
    df = pd.read_parquet(file_path)
    for k in asdict(empty_address).keys():
        assert k in df.columns
        assert pd.isna(df.loc[0, k]) or df.loc[0, k] is None


def test_to_dataframe_structure(filled_address):
    df = filled_address._to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == set(asdict(filled_address).keys())
    for k, v in asdict(filled_address).items():
        assert df.loc[0, k] == v


def test_dataframe_empty_model(empty_address):
    df = empty_address._to_dataframe()
    assert isinstance(df, pd.DataFrame)
    for k in asdict(empty_address).keys():
        assert k in df.columns
        assert pd.isna(df.loc[0, k]) or df.loc[0, k] is None


def test_roundtrip_json(tmp_path, filled_address):
    file_path = tmp_path / "roundtrip.json"
    filled_address.to_json(filepath=file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        loaded_data = json.load(f)
    new_model = CompanyAddressModel()
    for k, v in loaded_data.items():
        setattr(new_model, k, v)
    assert new_model.address1 == filled_address.address1
    assert new_model.city == filled_address.city
    assert new_model.country == filled_address.country
