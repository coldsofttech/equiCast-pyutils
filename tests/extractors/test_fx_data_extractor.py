from datetime import datetime

import pandas as pd
import pytest

from equicast_pyutils.extractors import FxDataExtractor
from equicast_pyutils.models.fx import FxConversionRateModel


class MockTicker:
    def __init__(self, ticker):
        self.ticker = ticker
        self.call_count = 0

    def history(self, period="1y", interval="1d"):
        self.call_count += 1
        if self.call_count < 3:
            return pd.DataFrame()

        return pd.DataFrame({
            "Close": [1.1, 1.2, 1.15],
            "Adj Close": [1.1, 1.2, 1.15]
        }, index=pd.to_datetime(["2023-09-01", "2023-09-02", "2023-09-03"]))


class EmptyTicker:
    def __init__(self, ticker):
        self.ticker = ticker

    def history(self, period="1y", interval="1d"):
        return pd.DataFrame()


@pytest.fixture
def patch_yf_ticker(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", MockTicker)


@pytest.fixture
def patch_empty_ticker(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", EmptyTicker)


def test_yf_obj_initialization_usd_to_x(patch_yf_ticker):
    extractor = FxDataExtractor(from_currency="USD", to_currency="GBP")
    yf_obj = extractor.yf_obj
    assert yf_obj.ticker == "GBP=X"


def test_yf_obj_initialization_x_to_usd(patch_yf_ticker):
    extractor = FxDataExtractor(from_currency="CHF", to_currency="USD")
    yf_obj = extractor.yf_obj
    assert yf_obj.ticker == "CHFUSD=X"


def test_yf_obj_initialization_x_to_y(patch_yf_ticker):
    extractor = FxDataExtractor(from_currency="GBP", to_currency="CHF")
    yf_obj = extractor.yf_obj
    assert yf_obj.ticker == "GBPCHF=X"


def test_get_history_with_fallback(patch_yf_ticker):
    extractor = FxDataExtractor(from_currency="USD", to_currency="EUR")
    history = extractor._get_history()
    assert not history.empty
    assert "Close" in history.columns
    assert len(history) == 3


def test_extract_fx_data_returns_model(patch_yf_ticker):
    extractor = FxDataExtractor(from_currency="USD", to_currency="JPY")
    model = extractor.extract_fx_data()
    assert isinstance(model, FxConversionRateModel)
    assert model.pair == "USDJPY"
    assert model.from_currency == "USD"
    assert model.to_currency == "JPY"
    assert len(model.rates) == 3
    for date_str, rate in model.rates.items():
        datetime.strptime(date_str, "%Y-%m-%d")
        assert isinstance(rate, float)


def test_no_data_found_raises_without_retry(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", EmptyTicker)
    extractor = FxDataExtractor(from_currency="USD", to_currency="AUD")

    _get_history_fn = FxDataExtractor._get_history.__wrapped__
    with pytest.raises(ValueError, match="No historical data found"):
        _get_history_fn(extractor, period="1y")
