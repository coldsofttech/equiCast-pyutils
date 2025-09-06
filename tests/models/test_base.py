import json
import os
import tempfile
from dataclasses import dataclass

import pandas as pd
import pytest

from equicast_pyutils.models.base import ExportableModel


@dataclass
class DummyModel(ExportableModel):
    """A dummy model for testing ExportableModel base functionality."""
    name: str
    value: int

    def _to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([{"name": self.name, "value": self.value}])


@pytest.fixture
def dummy_model():
    return DummyModel(name="test", value=42)


def test_to_json_string(dummy_model):
    json_str = dummy_model.to_json()
    data = json.loads(json_str)

    assert data["name"] == "test"
    assert data["value"] == 42


def test_to_json_file(dummy_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "dummy.json")
        dummy_model.to_json(filepath)

        assert os.path.exists(filepath)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["value"] == 42


def test_to_parquet_file(dummy_model):
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "dummy.parquet")
        dummy_model.to_parquet(filepath)

        assert os.path.exists(filepath)

        df = pd.read_parquet(filepath)
        assert df.iloc[0]["name"] == "test"
        assert df.iloc[0]["value"] == 42
