import json
import os

import pandas as pd
import pytest

from equicast_pyutils.models.stock import OHLCModel


@pytest.fixture
def sample_model():
    return OHLCModel(low=10, high=20, open=12, close=18)


def test_initialization(sample_model):
    assert sample_model.low == 10
    assert sample_model.high == 20
    assert sample_model.open == 12
    assert sample_model.close == 18
    assert sample_model.average == 15


def test_average_with_none():
    model = OHLCModel(low=None, high=20, open=12, close=18)
    assert model.average is None

    model = OHLCModel(low=10, high=None, open=12, close=18)
    assert model.average is None


def test_is_empty_property():
    model = OHLCModel(low=0, high=10, open=5, close=7)
    assert model.is_empty is True

    model = OHLCModel(low=5, high=10, open=6, close=9)
    assert model.is_empty is False


def test_to_json_string(sample_model):
    json_str = sample_model.to_json()
    data = json.loads(json_str)
    assert data["low"] == 10
    assert data["high"] == 20
    assert data["average"] == 15


def test_to_json_file(tmp_path, sample_model):
    file_path = tmp_path / "model.json"
    sample_model.to_json(filepath=file_path)
    assert os.path.exists(file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["low"] == 10
    assert data["average"] == 15


def test_to_parquet_file(tmp_path, sample_model):
    file_path = tmp_path / "model.parquet"
    sample_model.to_parquet(filepath=file_path)
    assert os.path.exists(file_path)
    df = pd.read_parquet(file_path)
    assert df.iloc[0]["low"] == 10
    assert df.iloc[0]["average"] == 15


def test_to_dataframe(sample_model):
    df = sample_model._to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]["low"] == 10
    assert df.iloc[0]["average"] == 15


def test_empty_model_average():
    model = OHLCModel(low=None, high=None, open=None, close=None)
    assert model.average is None
    assert model.is_empty is True


def test_edge_cases_zero_values():
    model = OHLCModel(low=0, high=0, open=0, close=0)
    assert model.average == 0
    assert model.is_empty is True


def test_export_consistency(tmp_path):
    model = OHLCModel(low=5, high=15, open=7, close=12)
    json_file = tmp_path / "model.json"
    parquet_file = tmp_path / "model.parquet"

    model.to_json(filepath=json_file)
    model.to_parquet(filepath=parquet_file)

    with open(json_file, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    df = pd.read_parquet(parquet_file)

    assert json_data["low"] == df.iloc[0]["low"]
    assert json_data["average"] == df.iloc[0]["average"]
