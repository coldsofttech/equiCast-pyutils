from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from _pytest.raises import raises

from equicast_pyutils.extractors import StockDataExtractor
from equicast_pyutils.models.stock import StockPriceModel, DividendModel, FundamentalsModel


@pytest.fixture
def sample_info():
    return {
        "currency": "USD",
        "quoteType": "EQUITY",
        "currentPrice": 150.0,
        "open": 148.0,
        "dayLow": 147.0,
        "dayHigh": 151.0,
        "fiftyTwoWeekLow": 120.0,
        "fiftyTwoWeekHigh": 180.0,
        "trailingPE": 20,
        "forwardPE": 25,
        "trailingEps": 5,
        "forwardEps": 6,
    }


@pytest.fixture
def sample_history():
    return pd.DataFrame({
        "Open": [148.0],
        "High": [151.0],
        "Low": [147.0],
        "Close": [150.0],
        "Adj Close": [150.0],
    }, index=[pd.Timestamp("2025-09-14")])


@pytest.fixture
def sample_financials():
    return pd.DataFrame({
        "Total Revenue": [500],
        "Gross Profit": [200],
        "Operating Income": [150],
        "Net Income": [100]
    }, index=[pd.Timestamp("2025-09-14")]).T


@pytest.fixture
def sample_balance_sheet():
    return pd.DataFrame({
        pd.Timestamp("2025-09-14"): {
            "Total Equity": 400,
            "Total Debt": 100,
            "Total Assets": 600
        }
    })


@pytest.fixture
def sample_cashflow():
    return pd.DataFrame({
        "Operating Cash Flow": [120],
        "Capital Expenditure": [20],
    }, index=["2025-09-14"])


@pytest.fixture
def mock_yf_ticker(sample_info, sample_history):
    mock_ticker = MagicMock()
    mock_ticker.info = sample_info
    mock_ticker.history.return_value = sample_history
    mock_ticker.dividends = pd.Series([0.5], index=[pd.Timestamp("2025-09-14")])
    mock_ticker.financials = pd.DataFrame()
    mock_ticker.balance_sheet = pd.DataFrame()
    mock_ticker.cash_flow = pd.DataFrame()
    return mock_ticker


@pytest.fixture
def no_retry(monkeypatch):
    monkeypatch.setattr("equicast_pyutils.extractors.retry.retry", lambda *args, **kwargs: (lambda f: f))


def test_lazy_loading_and_delisted(monkeypatch):
    with patch("yfinance.Ticker"):
        extractor = StockDataExtractor("AAPL")
        assert extractor._yf_obj is None
        extractor.yf_obj
        assert extractor._yf_obj is not None


def test_safe_methods():
    extractor = StockDataExtractor("AAPL")
    assert extractor._safe_get({}, "nonexistent", default=42) == 42
    assert extractor._safe_float("123.45") == 123.45
    assert extractor._safe_float("invalid", default=-1) == -1
    assert extractor._safe_int("42") == 42
    assert extractor._safe_int("invalid", default=-1) == -1


def test_get_history_fallback_skip_retry(monkeypatch, mock_yf_ticker, no_retry):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    mock_yf_ticker.history.return_value = pd.DataFrame()
    with raises(RuntimeError):
        extractor._get_history(period="1d")


def test_get_dividends(monkeypatch, mock_yf_ticker):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    dividends = extractor._get_dividends()
    assert not dividends.empty


def test_get_info(monkeypatch, mock_yf_ticker):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    info = extractor._get_info()
    assert info["currency"] == "USD"


def test_price_at_period(monkeypatch, mock_yf_ticker):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    price = extractor._get_price_at_period("1d", "close")
    assert price == 150.0
    with pytest.raises(ValueError):
        extractor._get_price_at_period("1d", "unsupported")


def test_extract_stock_price_data(monkeypatch, mock_yf_ticker):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    model = extractor.extract_stock_price_data()
    assert isinstance(model, StockPriceModel)
    assert model.ticker == "AAPL"


def test_extract_dividends(monkeypatch, mock_yf_ticker):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    model = extractor.extract_dividends()
    assert isinstance(model, DividendModel)
    assert model.ticker == "AAPL"


def test_extract_fundamentals(monkeypatch, mock_yf_ticker, sample_financials, sample_balance_sheet, sample_cashflow):
    extractor = StockDataExtractor("AAPL")
    extractor._yf_obj = mock_yf_ticker
    monkeypatch.setattr(extractor, "_get_financials", lambda: sample_financials)
    monkeypatch.setattr(extractor, "_get_balance_sheet", lambda: sample_balance_sheet)
    monkeypatch.setattr(extractor, "_get_cash_flow", lambda: sample_cashflow)

    model = extractor.extract_fundamentals()
    assert isinstance(model, FundamentalsModel)
    assert model.day.close == 150.0
    assert model.one_year.low == 120.0
    assert model.trailing_pe == 20
    assert model.forward_pe == 25
    assert model.gross_margin == pytest.approx(200 / 500 * 100)
    assert model.return_on_equity == pytest.approx(100 / 400 * 100)
    assert model.free_cash_flow_per_share is None
