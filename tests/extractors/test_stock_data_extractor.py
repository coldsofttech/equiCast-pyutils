import math
from datetime import datetime

import pandas as pd
import pytest

from equicast_pyutils.extractors.stock_data_extractor import StockDataExtractor
from equicast_pyutils.models.stock import StockPriceModel, CompanyAddressModel, CompanyOfficerModel, CompanyProfileModel


class MockTicker:
    def __init__(self, ticker):
        self.ticker = ticker
        self.call_count = 0
        self.info = {"currency": "USD"}

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
        self.info = {"currency": "USD"}

    def history(self, period="1y", interval="1d"):
        return pd.DataFrame()


class QuoteNoneTicker:
    def __init__(self, ticker):
        self.ticker = ticker
        self.info = {"currency": "USD", "quoteType": "NONE"}

    def history(self, period="1y", interval="1d"):
        return pd.DataFrame({
            "Close": [10.0],
            "Adj Close": [10.0]
        }, index=pd.to_datetime(["2023-09-01"]))


@pytest.fixture
def patch_yf_ticker(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", MockTicker)


@pytest.fixture
def patch_empty_ticker(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", EmptyTicker)


@pytest.fixture(autouse=True)
def patch_sleep(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda x: None)


@pytest.fixture
def sample_info():
    return {
        "longName": "Apple Inc.",
        "quoteType": "EQUITY",
        "exchange": "NASDAQ",
        "currency": "USD",
        "longBusinessSummary": "Apple Inc. CEO: Tim Cook",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "website": "https://apple.com",
        "beta": 1.2,
        "payoutRatio": 0.25,
        "dividendRate": 0.88,
        "dividendYield": 0.007,
        "marketCap": 2500000000000,
        "totalAssets": 3000000000000,
        "volume": 50000000,
        "address1": "1 Apple Park Way",
        "address2": "",
        "city": "Cupertino",
        "state": "CA",
        "zip": "95014",
        "country": "USA",
        "region": "US",
        "fullTimeEmployees": 154000,
        "companyOfficers": [{"name": "Tim Cook", "title": "CEO"}],
        "executiveTeam": [],
        "firstTradeDateMilliseconds": 345446400000
    }


@pytest.fixture
def extractor(monkeypatch, sample_info):
    ext = StockDataExtractor(ticker="AAPL")
    monkeypatch.setattr(ext, "_get_info", lambda: sample_info)
    return ext


def test_yf_obj_initialization(patch_yf_ticker):
    extractor = StockDataExtractor(ticker="AAPL")
    yf_obj = extractor.yf_obj
    assert yf_obj.ticker == "AAPL"


def test_get_history_with_fallback(patch_yf_ticker):
    extractor = StockDataExtractor(ticker="AAPL")
    history = extractor._get_history()
    assert not history.empty
    assert "Close" in history.columns
    assert len(history) == 3
    assert extractor.is_delisted is False


def test_extract_stock_price_returns_model(monkeypatch, patch_yf_ticker):
    extractor = StockDataExtractor(ticker="AAPL")
    monkeypatch.setattr(extractor, "_get_info", lambda: {"currency": "USD"})
    monkeypatch.setattr(
        extractor, "_get_history",
        lambda period="1y": pd.DataFrame({
            "Close": [1.1, 1.2, 1.15],
            "Adj Close": [1.1, 1.2, 1.15]
        }, index=pd.to_datetime(["2023-09-01", "2023-09-02", "2023-09-03"]))
    )

    model = extractor.extract_stock_price_data()

    assert isinstance(model, StockPriceModel)
    assert model.ticker == "AAPL"
    assert model.currency == "USD"
    assert len(model.prices) == 3
    for date_str, price in model.prices.items():
        datetime.strptime(date_str, "%Y-%m-%d")
        assert isinstance(price, float)

    assert extractor.is_delisted is False


def test_no_data_found_sets_delisted(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", EmptyTicker)
    extractor = StockDataExtractor(ticker="AAPL")

    _get_history_fn = StockDataExtractor._get_history.__wrapped__
    with pytest.raises(ValueError, match="No historical data found"):
        _get_history_fn(extractor, period="1y")

    assert extractor.is_delisted is True


def test_explicit_quoteType_none_sets_delisted(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", QuoteNoneTicker)

    extractor = StockDataExtractor(ticker="XYZ")
    extractor._yf_obj = QuoteNoneTicker("XYZ")
    extractor._check_delisted(info=extractor._yf_obj.info)
    assert extractor.is_delisted is True


def test_no_data_found_raises_without_retry(monkeypatch):
    monkeypatch.setattr("yfinance.Ticker", EmptyTicker)
    extractor = StockDataExtractor(ticker="AAPL")

    _get_history_fn = StockDataExtractor._get_history.__wrapped__
    with pytest.raises(ValueError, match="No historical data found"):
        _get_history_fn(extractor, period="1y")


def test_safe_float_int(extractor):
    assert extractor._safe_float("10.5") == 10.5
    assert extractor._safe_float("abc", 1.0) == 1.0
    assert extractor._safe_float(math.inf, 0.0) == 0.0
    assert extractor._safe_int("42") == 42
    assert extractor._safe_int("abc", 7) == 7
    assert extractor._safe_int(math.nan, 5) == 5


def test_extract_company_address(extractor):
    addr = extractor._extract_company_address()
    assert isinstance(addr, CompanyAddressModel)
    assert addr.address1 == "1 Apple Park Way"
    assert addr.city == "Cupertino"
    assert addr.country == "USA"
    assert addr.region == "US"


def test_get_ceos_deduplication(extractor, monkeypatch):
    original_get_info = extractor._get_info

    def patched_get_info():
        info = original_get_info()
        info["executiveTeam"] = [{"name": "Tim Cook", "title": "CEO"}]
        return info

    monkeypatch.setattr(extractor, "_get_info", patched_get_info)

    ceos = extractor._get_ceos()
    assert len(ceos) == 1
    assert isinstance(ceos[0], CompanyOfficerModel)
    assert ceos[0].name == "Tim Cook"


def test_get_ceos_from_summary(monkeypatch):
    info = {"longBusinessSummary": "Apple Inc. Chief Executive Officer: Tim Cook"}
    ext = StockDataExtractor(ticker="AAPL")
    monkeypatch.setattr(ext, "_get_info", lambda: info)
    ceos = ext._get_ceos()
    assert len(ceos) == 1
    assert ceos[0].name == "Tim Cook"
    assert ceos[0].title == "CEO"


def test_extract_company_profile(extractor):
    profile = extractor.extract_company_profile()
    assert isinstance(profile, CompanyProfileModel)
    assert profile.ticker == "AAPL"
    assert profile.exchange == "NASDAQ"
    assert profile.address.city == "Cupertino"
    assert profile.ceos[0].name == "Tim Cook"
    assert isinstance(profile.ipo_date, datetime)
    assert profile.ipo_date.year == 1980
