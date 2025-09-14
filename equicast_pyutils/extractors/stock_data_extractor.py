import math
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import yfinance as yf

from equicast_pyutils.extractors.retry import retry
from equicast_pyutils.models.stock import StockPriceModel, CompanyProfileModel, CompanyAddressModel, DividendModel, \
    CompanyOfficerModel


@dataclass
class StockDataExtractor:
    """Stock Data Extractor"""
    ticker: str
    _yf_obj: yf.Ticker = field(default=None, init=False, repr=False)
    _is_delisted: bool = field(default=False, init=False)

    @property
    def is_delisted(self):
        return self._is_delisted

    @property
    def yf_obj(self):
        """Lazy initialisation of yfinance object."""
        if self._yf_obj is None:
            try:
                self._yf_obj = yf.Ticker(self.ticker)
            except Exception as e:
                raise ValueError(f"Failed to create yfinance object for {self.ticker}: {e}")
        return self._yf_obj

    def _check_delisted(self, info=None, history=None):
        try:
            if info is None:
                info = self._yf_obj.info if self._yf_obj else {}
            if history is None and self._yf_obj:
                history = self._yf_obj.history(period="1y")

            is_delisted = False

            # Case 1: Empty info
            if not info or len(info) < 5:
                is_delisted = True

            # Case 2: No price history at all
            if history is not None and history.empty:
                is_delisted = True

            # Case 3: Explicit signal
            if info.get("quoteType", "").lower() == "none":
                is_delisted = True

            self._is_delisted = is_delisted
        except Exception:
            self._is_delisted = True

    def _safe_get(self, info, key, default=None):
        try:
            return info.get(key, default)
        except Exception:
            return default

    def _safe_float(self, val, default: float = 0.0):
        try:
            val = float(val)
            if math.isnan(val) or math.isinf(val):
                return default
            return val
        except Exception:
            return default

    def _safe_int(self, val, default: int = 0):
        try:
            val = int(val)
            if math.isnan(val) or math.isinf(val):
                return default
            return val
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
            self._check_delisted(history=data)
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

        self._check_delisted(info=info)
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
        )

    def _extract_company_address(self, info=None):
        if info is None:
            info = self._get_info()

        model = CompanyAddressModel()
        model.address1 = self._safe_get(info, "address1", "")
        model.address2 = self._safe_get(info, "address2", "")
        model.city = self._safe_get(info, "city", "")
        model.state = self._safe_get(info, "state", "")
        model.zip = self._safe_get(info, "zip", "")
        model.country = self._safe_get(info, "country", "")
        model.region = self._safe_get(info, "region", "")

        return model

    def _get_ceos(self, info=None):
        if info is None:
            info = self._get_info()

        ceos = []
        seen_names = set()

        # 1. From companyOfficers
        officers = self._safe_get(info, "companyOfficers", [])
        for officer in officers:
            title = self._safe_get(officer, "title", "")
            if "ceo" in title.lower() or "chief executive officer" in title.lower():
                name = self._safe_get(officer, "name", "").strip()
                if name and name not in seen_names:
                    model = CompanyOfficerModel()
                    model.name, model.title = name, title
                    ceos.append(model)
                    seen_names.add(name)

        # 2. From executiveTeam
        exec_teams = self._safe_get(info, "executiveTeam", [])
        for exec_team in exec_teams:
            title = self._safe_get(exec_team, "title", "")
            if "ceo" in title.lower() or "chief executive officer" in title.lower():
                name = self._safe_get(exec_team, "name", "").strip()
                if name and name not in seen_names:
                    model = CompanyOfficerModel()
                    model.name, model.title = name, title
                    ceos.append(model)
                    seen_names.add(name)

        # 3. Fallback: from longBusinessSummary if both companyOfficers and executiveTeam is empty
        if not ceos:
            summary = self._safe_get(info, "longBusinessSummary", "")
            matches = re.findall(r'CEO\s*[:\-]?\s*([\w\s]+)', summary, re.I)
            matches += re.findall(r'Chief Executive Officer\s*[:\-]?\s*([\w\s]+)', summary, re.I)
            for m in matches:
                name = m.strip()
                if name and name not in seen_names:
                    model = CompanyOfficerModel()
                    model.name, model.title = name, "CEO"
                    ceos.append(model)
                    seen_names.add(name)

        return ceos

    def extract_company_profile(self):
        info = self._get_info()
        model = CompanyProfileModel(ticker=self.ticker)
        model.name = self._safe_get(info, "longName", "")
        model.quote_type = self._safe_get(info, "quoteType", "").upper()
        model.exchange = self._safe_get(info, "exchange", "")
        model.currency = self._safe_get(info, "currency", "")
        model.description = self._safe_get(info, "longBusinessSummary", "")
        model.sector = (
            self._safe_get(info, "sector", "")
            if model.quote_type.lower() == "equity" else model.quote_type
        )
        model.industry = (
            self._safe_get(info, "industry", "")
            if model.quote_type.lower() == "equity" else model.quote_type
        )
        model.website = self._safe_get(info, "website", "")
        model.beta = self._safe_float(self._safe_get(info, "beta", ""))
        model.payout_ratio = self._safe_float(self._safe_get(info, "payoutRatio", ""))
        model.dividend_rate = self._safe_float(self._safe_get(info, "dividendRate", ""))
        model.dividend_yield = self._safe_float(self._safe_get(info, "dividendYield", ""))
        model.volume = self._safe_int(self._safe_get(info, "volume", ""))
        model.market_cap = (
            self._safe_int(self._safe_get(info, "totalAssets", ""))
            if model.quote_type == "ETF" else self._safe_int(self._safe_get(info, "marketCap", ""))
        )
        model.address = self._extract_company_address(info=info)
        model.full_time_employees = self._safe_int(self._safe_get(info, "fullTimeEmployees", ""))
        model.ceos = self._get_ceos(info=info)
        model.ipo_date = (
            datetime.fromtimestamp(ipo_ts_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
            if (ipo_ts_ms := self._safe_get(info, "firstTradeDateMilliseconds", None)) else None
        )

        return model
