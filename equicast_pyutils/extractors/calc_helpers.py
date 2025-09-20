import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List, Dict

import numpy as np
import pandas as pd


@dataclass
class CalcHelpers:
    @staticmethod
    def infer_periods_per_year(idx: pd.Index) -> Optional[int]:
        if not isinstance(idx, (pd.DatetimeIndex, pd.PeriodIndex)):
            return None
        freq = pd.infer_freq(idx)
        if freq is None:
            return None
        mapping = {
            "B": 252, "D": 252,
            "W": 52,
            "M": 12,
            "Q": 4,
            "A": 1, "Y": 1,
        }
        if freq in mapping:
            return mapping[freq]
        for k, v in mapping.items():
            if freq.startswith(k):
                return v
        return None

    @staticmethod
    def calculate_volatility(
            prices: pd.Series,
            periods_per_year: Optional[int] = None,
            return_type: str = "log",
            ddof: int = 1,
            as_percent: bool = False,
    ) -> float:
        if prices is None or len(prices) < 2:
            raise ValueError("Need at least two price observations")

        s = prices.dropna().astype(float)
        if len(s) < 2:
            raise ValueError("Insufficient non-NaN data")

        if periods_per_year is None:
            periods_per_year = CalcHelpers.infer_periods_per_year(s.index) or 252

        if return_type == "log":
            returns = np.log(s / s.shift(1)).dropna()
        else:
            returns = s.pct_change().dropna()

        sigma = returns.std(ddof=ddof)
        annualized = float(sigma * math.sqrt(periods_per_year))
        return (annualized * 100) if as_percent else annualized

    @staticmethod
    def calculate_sharpe_ratio(
            prices: pd.Series,
            risk_free_rate: float = 0.0,
            periods_per_year: Optional[int] = None,
            return_type: str = "log",
            ddof: int = 1,
            as_percent: bool = False,
    ) -> float:
        if prices is None or len(prices) < 2:
            raise ValueError("Need at least two price observations")

        s = prices.dropna().astype(float)
        if len(s) < 2:
            raise ValueError("Insufficient non-NaN data")

        ppy = periods_per_year
        if ppy is None:
            ppy = CalcHelpers.infer_periods_per_year(s.index) or 252

        if return_type == "log":
            returns = np.log(s / s.shift(1)).dropna()
        elif return_type == "simple":
            returns = s.pct_change().dropna()
        else:
            raise ValueError("return_type must be 'log' or 'simple'")

        rf_periodic = (1 + risk_free_rate) ** (1 / ppy) - 1

        excess_returns = returns - rf_periodic

        sigma = excess_returns.std(ddof=ddof)
        sharpe_ratio = excess_returns.mean() / sigma * math.sqrt(ppy)

        return sharpe_ratio * 100 if as_percent else sharpe_ratio

    @staticmethod
    def calculate_max_drawdown(prices: pd.Series, as_percent: bool = False) -> float:
        if prices is None or len(prices) < 2:
            raise ValueError("Price series must have at least two observations")

        s = prices.dropna().astype(float)
        if len(s) < 2:
            raise ValueError("Insufficient non-NaN data")

        cum_max = s.cummax()
        drawdowns = (s - cum_max) / cum_max
        mdd = drawdowns.min()

        if as_percent:
            return abs(mdd) * 100
        return mdd

    @staticmethod
    def calculate_cagr(
            prices: pd.Series,
            periods: List[int] = [1, 2, 5, 10, 15, 20],
            as_percent: bool = False,
            end_date: pd.Timestamp = None
    ) -> Dict[str, float]:
        if prices is None or len(prices) < 2:
            raise ValueError("Price series must have at least two observations")

        s = prices.dropna().sort_index()
        if len(s) < 2:
            raise ValueError("Insufficient non-NaN data")

        if end_date is None:
            end_date = s.index[-1]
        elif isinstance(end_date, datetime):
            end_date = pd.Timestamp(end_date)

        cagr_dict = {}
        for period in periods:
            start_date = end_date - pd.DateOffset(years=period)
            s_start = s[s.index <= start_date]
            if s_start.empty:
                cagr_dict[f"{period}y"] = None
                continue

            start_price = s_start.iloc[-1]
            end_price = s[s.index <= end_date].iloc[-1]

            t = (end_date - s_start.index[-1]).days / 365.25  # actual years
            if t <= 0:
                cagr_dict[f"{period}y"] = None
                continue

            cagr = (end_price / start_price) ** (1 / t) - 1
            if as_percent:
                cagr *= 100

            cagr_dict[f"{period}y"] = round(cagr, 6)

        return cagr_dict

    @staticmethod
    def forecast_fx_prices(ohlc_history: pd.DataFrame, requested_days: int = 365 * 20) -> pd.DataFrame:
        if ohlc_history is None or ohlc_history.empty:
            raise ValueError("Historical OHLC data is required")

        ohlc = ohlc_history[['Open', 'High', 'Low', 'Close']].dropna()
        if len(ohlc) < 2:
            raise ValueError("At least 2 historical OHLC records are required")

        max_days = len(ohlc)
        forecast_days = min(requested_days, max_days)

        close_prices = ohlc['Close'].astype(float)
        returns = np.log(close_prices / close_prices.shift(1)).dropna()
        mu = returns.mean()
        sigma = returns.std()

        high_offsets = (ohlc['High'] - ohlc['Open']) / ohlc['Open']
        low_offsets = (ohlc['Low'] - ohlc['Open']) / ohlc['Open']

        start_date = ohlc.index[-1]
        forecast_dates = [start_date + timedelta(days=i + 1) for i in range(forecast_days)]
        forecast_df = pd.DataFrame(index=forecast_dates, columns=['Open', 'High', 'Low', 'Close'])
        last_close = close_prices.iloc[-1]
        np.random.seed(42)

        for date in forecast_dates:
            z = np.random.normal()
            close_forecast = last_close * np.exp((mu - 0.5 * sigma ** 2) + sigma * z)
            open_forecast = last_close
            high_forecast = open_forecast * (1 + np.random.choice(high_offsets))
            low_forecast = open_forecast * (1 + np.random.choice(low_offsets))

            high_forecast = max(high_forecast, open_forecast, close_forecast)
            low_forecast = min(low_forecast, open_forecast, close_forecast)

            forecast_df.loc[date] = [
                round(open_forecast, 6),
                round(high_forecast, 6),
                round(low_forecast, 6),
                round(close_forecast, 6)
            ]

            last_close = close_forecast

        return forecast_df
