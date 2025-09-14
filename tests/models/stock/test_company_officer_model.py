import json
import os
import tempfile

import pandas as pd
import pytest

from equicast_pyutils.models.stock import CompanyOfficerModel


@pytest.fixture
def officer_model():
    model = CompanyOfficerModel()
    model.name = "John Doe"
    model.title = "CEO"
    return model


@pytest.fixture
def empty_officer_model():
    return CompanyOfficerModel()


def test_is_empty_true(empty_officer_model):
    assert empty_officer_model.is_empty is True


def test_is_empty_false(officer_model):
    assert officer_model.is_empty is False


def test_to_json_string(officer_model):
    json_str = officer_model.to_json()
    data = json.loads(json_str)
    assert data["name"] == "John Doe"
    assert data["title"] == "CEO"


def test_to_json_file(officer_model):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
        path = tmp.name

    try:
        json_str = officer_model.to_json(filepath=path)
        assert os.path.exists(path)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["name"] == "John Doe"
        assert data["title"] == "CEO"
        assert json.loads(json_str) == data
    finally:
        os.remove(path)


def test_to_parquet(officer_model):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
        path = tmp.name

    try:
        officer_model.to_parquet(path)
        assert os.path.exists(path)
        df = pd.read_parquet(path)
        assert "name" in df.columns
        assert "title" in df.columns
        assert df.iloc[0]["name"] == "John Doe"
        assert df.iloc[0]["title"] == "CEO"
    finally:
        os.remove(path)


def test_to_parquet_empty_model(empty_officer_model):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
        path = tmp.name

    try:
        empty_officer_model.to_parquet(path)
        df = pd.read_parquet(path)
        assert "name" in df.columns
        assert "title" in df.columns
        assert df.iloc[0]["name"] is None
        assert df.iloc[0]["title"] is None
    finally:
        os.remove(path)


def test_to_dataframe_structure(officer_model):
    df = officer_model._to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["name", "title"]
    assert df.iloc[0]["name"] == "John Doe"
    assert df.iloc[0]["title"] == "CEO"


def test_to_dataframe_empty(empty_officer_model):
    df = empty_officer_model._to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["name", "title"]
    assert df.iloc[0]["name"] is None
    assert df.iloc[0]["title"] is None


def test_to_json_handles_non_ascii():
    model = CompanyOfficerModel()
    model.name = "José Álvarez"
    model.title = "CFO"
    json_str = model.to_json()
    data = json.loads(json_str)
    assert data["name"] == "José Álvarez"
    assert data["title"] == "CFO"
