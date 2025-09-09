import random
import time
from dataclasses import dataclass, field

import yfinance as yf

from equicast_pyutils.extractors.retry import retry
from equicast_pyutils.models.stock import StockPriceModel


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

    def extract_stock_price_data(self) -> StockPriceModel:
        history = self._get_history(period="max")
        price_col = "Adj Close" if "Adj Close" in history.columns else "Close"
        prices_dict = {d.strftime("%Y-%m-%d"): float(p) for d, p in history[price_col].items()}

        return StockPriceModel(
            ticker=self.ticker,
            prices=prices_dict
        )
