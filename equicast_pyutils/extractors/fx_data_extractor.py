from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf

from equicast_pyutils.extractors.calc_helpers import CalcHelpers
from equicast_pyutils.extractors.get_helpers import GetHelpers
from equicast_pyutils.extractors.safe_helpers import SafeHelpers
from equicast_pyutils.models import OHLCModel, MetadataModel
from equicast_pyutils.models.fx import FxPriceModel, FxProfileModel, FxFundamentalModel, FxCalculationModel, \
    FxForecastModel


@dataclass
class FxDataExtractor:
    from_currency: str
    to_currency: str
    period: Optional[str] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    _yf_obj: yf.Ticker = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if self.period and (self.start_date or self.end_date):
            raise ValueError("period and start_date/end_date cannot both be specified")

        if self.period and self.period not in ["1y", "5y", "10y", "15y", "20y", "max"]:
            raise ValueError("period must be one of '1y', '5y', '10y', '15y', '20y', 'max'")

        if self.period is None:
            today = datetime.now(tz=timezone.utc)
            if self.start_date and self.end_date is None:
                self.end_date = today
            elif self.end_date and self.start_date is None:
                self.start_date = self.end_date - timedelta(days=365)
            elif self.start_date is None and self.end_date is None:
                self.period = "max"

    @property
    def yf_obj(self):
        if self._yf_obj is None:
            if self.from_currency == "USD":
                ticker = f"{self.to_currency}=X"
            else:
                ticker = f"{self.from_currency}{self.to_currency}=X"

            try:
                self._yf_obj = yf.Ticker(ticker)
            except Exception as e:
                raise ValueError(f"Failed to create yfinance object for {ticker}: {e}")
        return self._yf_obj

    def extract_fx_prices(self) -> FxPriceModel:
        if self.period:
            history = GetHelpers.get_history(self.yf_obj, period=self.period)
        else:
            history = GetHelpers.get_history(self.yf_obj, start=self.start_date, end=self.end_date)

        ohlc_list = []
        for date, row in history.iterrows():
            ohlc = OHLCModel(
                date=date.to_pydatetime(),
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
                adj_close=row["Adj Close"] if "Adj Close" in row else None,
                volume=int(row["Volume"]) if not pd.isna(row["Volume"]) else None,
            )
            ohlc_list.append(ohlc)

        metadata = MetadataModel(source="yfinance")
        fx_price = FxPriceModel(
            from_currency=self.from_currency,
            to_currency=self.to_currency,
            prices=ohlc_list,
            metadata=metadata,
        )

        return fx_price

    def extract_fx_profile(self) -> FxProfileModel:
        info = GetHelpers.get_info(self.yf_obj)

        metadata = MetadataModel(source="yfinance")
        fx_profile = FxProfileModel(
            from_currency=self.from_currency,
            to_currency=self.to_currency,
            exchange=SafeHelpers.safe_get(info, "exchange", None),
            region=SafeHelpers.safe_get(info, "region", None),
            quote_type=SafeHelpers.safe_get(info, "quoteType", None),
            description=SafeHelpers.safe_get(info, "longName", None),
            metadata=metadata,
        )

        return fx_profile

    def extract_fx_fundamentals(self) -> FxFundamentalModel:
        info = GetHelpers.get_info(self.yf_obj)

        day = OHLCModel(
            low=SafeHelpers.safe_float(
                SafeHelpers.safe_get(info, "dayLow",
                                     GetHelpers.get_price_at_period(self.yf_obj, period="1d", parameter="low"))
            ),
            high=SafeHelpers.safe_float(
                SafeHelpers.safe_get(info, "dayHigh",
                                     GetHelpers.get_price_at_period(self.yf_obj, period="1d", parameter="high"))
            ),
            open=SafeHelpers.safe_float(
                SafeHelpers.safe_get(info, "open",
                                     GetHelpers.get_price_at_period(self.yf_obj, period="1d", parameter="open"))
            ),
            close=SafeHelpers.safe_float(
                SafeHelpers.safe_get(info, "currentPrice",
                                     GetHelpers.get_price_at_period(self.yf_obj, period="1d", parameter="close"))
            )
        )

        year = OHLCModel(
            low=SafeHelpers.safe_float(
                SafeHelpers.safe_get(info, "fiftyTwoWeekLow",
                                     GetHelpers.get_price_at_period(self.yf_obj, period="1y", parameter="low"))
            ),
            high=SafeHelpers.safe_float(
                SafeHelpers.safe_get(info, "fiftyTwoWeekHigh",
                                     GetHelpers.get_price_at_period(self.yf_obj, period="1y", parameter="high"))
            ),
            open=GetHelpers.get_price_at_period(self.yf_obj, period="1y", parameter="open"),
            close=GetHelpers.get_price_at_period(self.yf_obj, period="1y", parameter="close")
        )

        metadata = MetadataModel(source="yfinance")
        fx_fundamental = FxFundamentalModel(
            from_currency=self.from_currency,
            to_currency=self.to_currency,
            ma50=SafeHelpers.safe_float(SafeHelpers.safe_get(info, "fiftyDayAverage", None)),
            ma200=SafeHelpers.safe_float(SafeHelpers.safe_get(info, "twoHundredDayAverage", None)),
            day=day,
            year=year,
            metadata=metadata,
        )

        return fx_fundamental

    def extract_fx_calculations(self) -> FxCalculationModel:
        history_1y = GetHelpers.get_history(self.yf_obj, period="1y")
        history_max = GetHelpers.get_history(self.yf_obj, period="max")

        volatility = CalcHelpers.calculate_volatility(history_1y["Close"], return_type="log")
        sharpe_ratio = CalcHelpers.calculate_sharpe_ratio(history_1y["Close"], risk_free_rate=0.0, return_type="log")
        max_drawdown = CalcHelpers.calculate_max_drawdown(history_1y["Close"])
        cagr = CalcHelpers.calculate_cagr(history_max["Close"], periods=[1, 5])

        metadata = MetadataModel(source="yfinance")

        model = FxCalculationModel(
            from_currency=self.from_currency,
            to_currency=self.to_currency,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            cagr_1y=cagr["1y"],
            cagr_5y=cagr["5y"],
            metadata=metadata,
        )

        return model

    def extract_fx_forecast(self) -> FxForecastModel:
        history = GetHelpers.get_history(self.yf_obj, period="max")

        forecast = CalcHelpers.forecast_fx_prices(history, requested_days=365 * 20)
        ohlc_list = []
        for date, row in forecast.iterrows():
            ohlc = OHLCModel(
                date=date.to_pydatetime(),
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
            )
            ohlc_list.append(ohlc)

        metadata = MetadataModel(source="yfinance")
        model = FxForecastModel(
            from_currency=self.from_currency,
            to_currency=self.to_currency,
            prices=ohlc_list,
            model="GBM (Geometric Brownian Motion)",
            metadata=metadata,
        )

        return model
