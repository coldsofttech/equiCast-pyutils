import random
import time
from dataclasses import dataclass, field

import yfinance as yf

from equicast_pyutils.extractors.retry import retry
from equicast_pyutils.models.stock import StockPriceModel
from equicast_pyutils.models.stock.dividend_model import DividendModel


@dataclass
class StockDataExtractor:
    """Stock Data Extractor"""
    ticker: str
    _yf_obj: yf.Ticker = field(default=None, init=False, repr=False)

    @property
    def yf_obj(self):
        """Lazy initialisation of yfinance object."""
        if self._yf_obj is None:
            try:
                self._yf_obj = yf.Ticker(self.ticker)
            except Exception as e:
                raise ValueError(f"Failed to create yfinance object for {self.ticker}: {e}")
        return self._yf_obj

    def _safe_get(self, info, key, default=None):
        try:
            return info.get(key, default)
        except Exception:
            return default

    @retry(delay=2)
    def _get_history(self, period="1y", interval="1d"):
        time.sleep(random.uniform(0.1, 0.5))
        data = self.yf_obj.history(period=period, interval=interval)

        fallback_periods = ["20y", "15y", "10y", "5y", "1y"]
        for fallback in fallback_periods:
            if data.empty:
                data = self.yf_obj.history(period=fallback, interval=interval)
                print(f"⏳ No data found. Fallback to {fallback}.")
            else:
                break

        if data.empty:
            raise ValueError("No historical data found for the specified ticker.")
        return data

    @retry(delay=2)
    def _get_dividends(self):
        time.sleep(random.uniform(0.1, 0.5))
        return self.yf_obj.dividends

    @retry(delay=2)
    def _get_info(self):
        time.sleep(random.uniform(0.1, 0.5))
        info = self.yf_obj.info
        if not info or len(info) < 5:
            info = self.yf_obj.get_info()

        if not info or len(info) < 5:
            raise ValueError("No info found for the specified ticker.")
        return info

    def extract_stock_price_data(self) -> StockPriceModel:
        history = self._get_history(period="max")
        price_col = "Adj Close" if "Adj Close" in history.columns else "Close"
        prices_dict = {d.strftime("%Y-%m-%d"): float(p) for d, p in history[price_col].items()}
        info = self._get_info()
        currency = self._safe_get(info, "currency", "")

        return StockPriceModel(
            ticker=self.ticker,
            prices=prices_dict,
            currency=currency
        )

    def extract_dividends(self):
        dividends = self._get_dividends()
        prices_dict = {d.strftime("%Y-%m-%d"): float(p) for d, p in dividends.items()}
        info = self._get_info()
        currency = self._safe_get(info, "currency", "")

        return DividendModel(
            ticker=self.ticker,
            prices=prices_dict,
            currency=currency
        ) if len(prices_dict) > 0 else None
