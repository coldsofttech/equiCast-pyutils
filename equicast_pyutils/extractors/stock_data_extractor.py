import math
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import yfinance as yf

from equicast_pyutils.extractors.retry import retry
from equicast_pyutils.models.stock import StockPriceModel, CompanyProfileModel, CompanyAddressModel, DividendModel, \
    CompanyOfficerModel, FundamentalsModel, OHLCModel


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

    @retry(delay=2)
    def _get_financials(self):
        time.sleep(random.uniform(0.1, 0.5))
        financials = self.yf_obj.financials
        if financials.empty:
            financials = self.yf_obj.get_financials()

        return financials

    @retry(delay=2)
    def _get_balance_sheet(self):
        time.sleep(random.uniform(0.1, 0.5))
        balance_sheet = self.yf_obj.balance_sheet
        if balance_sheet.empty:
            balance_sheet = self.yf_obj.get_balance_sheet()

        return balance_sheet

    @retry(delay=2)
    def _get_cash_flow(self):
        time.sleep(random.uniform(0.1, 0.5))
        cash_flow = self.yf_obj.cash_flow
        if cash_flow.empty:
            cash_flow = self.yf_obj.get_cash_flow()

        return cash_flow

    def _get_price_at_period(self, period: str = "1d", parameter: str = "close"):
        param_map = {
            "close": ["Adj Close", "Close"],
            "open": ["Open"],
            "low": ["Low"],
            "high": ["High"],
        }

        parameter = parameter.lower()
        if parameter not in param_map:
            raise ValueError(f"Unsupported parameter: {parameter}")

        u_period = "5d" if period == "1d" else period
        history = self._get_history(period=u_period)
        if history.empty:
            return None

        first_row = history.iloc[-1] if period == "1d" else history.iloc[0]
        for col in param_map[parameter]:
            if col in history.columns:
                return float(first_row[col])

        return None

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

    def _get_peg(self, info, fundamentals: FundamentalsModel):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        pe = fundamentals.forward_pe or fundamentals.trailing_pe
        if not pe:
            return None

        # 1. Analyst growth
        growth_rate = self._safe_float(self._safe_get(info, "earningsQuarterlyGrowth", ""))
        if growth_rate and 0 < growth_rate < 1:
            growth_rate = (1 + growth_rate) ** 4 - 1

        # 2. Fallback: estimate from Revenue Growth
        if not growth_rate:
            growth_rate = self._safe_float(self._safe_get(info, "revenueGrowth", ""))
            if growth_rate and 0 < growth_rate < 1:
                growth_rate = (1 + growth_rate) ** 4 - 1

        # 3. Final fallback: no growth info > PEG invalid
        if not growth_rate or growth_rate <= 0:
            return None

        return pe / growth_rate

    def _get_price_to_book(self, info):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        pb = self._safe_float(self._safe_get(info, "priceToBook", ""))
        if pb is not None:
            return pb

        # Fallback
        market_cap = self._safe_int(self._safe_get(info, "marketCap", ""))
        book_value = self._safe_float(self._safe_get(info, "bookValue", ""))
        shares_outstanding = self._safe_int(self._safe_get(info, "shareOutstanding", ""))
        if market_cap and book_value and shares_outstanding:
            total_equity = book_value * shares_outstanding
            if total_equity > 0:
                return market_cap / total_equity

        return None

    def _get_price_to_sales(self, info):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        ps = self._safe_float(self._safe_get(info, "priceToSalesTrailing12Months", ""))
        if ps is not None:
            return ps

        # Fallback
        market_cap = self._safe_int(self._safe_get(info, "marketCap", ""))
        total_revenue = self._safe_int(self._safe_get(info, "totalRevenue", ""))
        if market_cap and total_revenue and total_revenue > 0:
            return market_cap / total_revenue

        return None

    def _get_ev_ebitda(self, info):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        market_cap = self._safe_int(self._safe_get(info, "marketCap", ""))
        total_debt = self._safe_int(self._safe_get(info, "totalDebt", "")) or 0
        total_cash = (
                self._safe_int(self._safe_get(info, "totalCash", "")) or
                self._safe_int(self._safe_get(info, "cash", "")) or 0
        )
        ebitda = self._safe_int(self._safe_get(info, "ebitda", ""))

        if not market_cap or not ebitda:
            return None

        ev = market_cap + total_debt - total_cash
        if ebitda == 0:
            return None

        return ev / ebitda

    def _get_gross_margin(self, info, financials):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        gross_margin = self._safe_float(self._safe_get(info, "grossMargins", ""))
        if gross_margin:
            return gross_margin * 100

        # Fallback
        if financials.empty:
            return None

        latest = financials.iloc[:, 0]
        revenue = (
                self._safe_float(self._safe_get(latest, "Total Revenue", "")) or
                self._safe_float(self._safe_get(latest, "Revenue", "")) or
                self._safe_float(self._safe_get(latest, "Sales", ""))
        )
        gross_profit = self._safe_float(self._safe_get(latest, "Gross Profit", ""))

        if gross_profit is not None and revenue:
            return (gross_profit / revenue) * 100

        return None

    def _get_operating_margin(self, info, financials):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        operating_margin = self._safe_float(self._safe_get(info, "operatingMargins", ""))
        if operating_margin:
            return operating_margin * 100

        # Fallback
        if financials.empty:
            return None

        latest = financials.iloc[:, 0]
        revenue = (
                self._safe_float(self._safe_get(latest, "Total Revenue", "")) or
                self._safe_float(self._safe_get(latest, "Revenue", "")) or
                self._safe_float(self._safe_get(latest, "Sales", ""))
        )
        operating_income = (
                self._safe_float(self._safe_get(latest, "Operating Income", "")) or
                self._safe_float(self._safe_get(latest, "Operating Profit", ""))
        )

        if operating_income is not None and revenue:
            return (operating_income / revenue) * 100

        return None

    def _get_profit_margin(self, info, financials):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        profit_margin = self._safe_float(self._safe_get(info, "profitMargins", ""))
        if profit_margin:
            return profit_margin * 100

        # Fallback
        if financials.empty:
            return None

        latest = financials.iloc[:, 0]
        revenue = (
                self._safe_float(self._safe_get(latest, "Total Revenue", "")) or
                self._safe_float(self._safe_get(latest, "Revenue", "")) or
                self._safe_float(self._safe_get(latest, "Sales", ""))
        )
        net_income = (
                self._safe_float(self._safe_get(latest, "Net Income", "")) or
                self._safe_float(self._safe_get(latest, "Net Income Applicable To Common Shares", ""))
        )

        if net_income is not None and revenue:
            return (net_income / revenue) * 100

        return None

    def _get_return_on_equity(self, info, financials, balance_sheet):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        roe = self._safe_float(self._safe_get(info, "returnOnEquity", ""))
        if roe:
            return roe * 100

        # Fallback
        if financials.empty or balance_sheet.empty:
            return None

        latest_fin = financials.iloc[:, 0]
        latest_bs = balance_sheet.iloc[:, 0]
        net_income = (
                self._safe_float(self._safe_get(latest_fin, "Net Income", "")) or
                self._safe_float(self._safe_get(latest_fin, "NetIncome", ""))
        )
        shareholder_equity = (
                self._safe_float(self._safe_get(latest_bs, "Stockholders Equity", "")) or
                self._safe_float(self._safe_get(latest_bs, "Total Stockholder Equity", "")) or
                self._safe_float(self._safe_get(latest_bs, "Total Equity", ""))
        )

        if net_income is not None and shareholder_equity:
            return (net_income / shareholder_equity) * 100

        return None

    def _get_return_on_assets(self, info, financials, balance_sheet):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        roa = self._safe_float(self._safe_get(info, "returnOnAssets", ""))
        if roa:
            return roa * 100

        # Fallback
        if financials.empty or balance_sheet.empty:
            return None

        latest_fin = financials.iloc[:, 0]
        latest_bs = balance_sheet.iloc[:, 0]
        net_income = (
                self._safe_float(self._safe_get(latest_fin, "Net Income", "")) or
                self._safe_float(self._safe_get(latest_fin, "NetIncome", ""))
        )
        total_assets = self._safe_float(self._safe_get(latest_bs, "Total Assets", ""))

        if net_income is not None and total_assets:
            return (net_income / total_assets) * 100

        return None

    def _get_debt_to_equity(self, info, balance_sheet):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        de_ratio = self._safe_float(self._safe_get(info, "debtToEquity", ""))
        if de_ratio:
            return de_ratio

        # Fallback
        if balance_sheet.empty:
            return None

        latest = balance_sheet.iloc[:, 0]
        shareholder_equity = (
                self._safe_float(self._safe_get(latest, "Stockholders Equity", "")) or
                self._safe_float(self._safe_get(latest, "Total Stockholder Equity", "")) or
                self._safe_float(self._safe_get(latest, "Total Equity", ""))
        )
        total_debt = (
                self._safe_float(self._safe_get(latest, "Total Debt", "")) or
                self._safe_float(self._safe_get(latest, "Short Long Term Debt", ""))
        )

        if total_debt is not None and shareholder_equity:
            return total_debt / shareholder_equity

        return None

    def _get_free_cash_flow_per_share(self, info, cash_flow):
        quote_type = self._safe_get(info, "quoteType", "").lower()
        if quote_type in ["etf", "mutualfund"]:
            return None

        if cash_flow.empty:
            return None

        latest = cash_flow.iloc[:, 0]
        operating_cf = self._safe_float(self._safe_get(latest, "Operating Cash Flow", ""))
        capex = self._safe_float(self._safe_get(latest, "Capital Expenditure", ""))

        if operating_cf is None or capex is None:
            return None

        fcf = operating_cf - capex
        shares_outstanding = self._safe_int(self._safe_get(info, "sharesOutstanding", ""))
        if shares_outstanding is None or shares_outstanding == 0:
            return None

        if fcf is not None and shares_outstanding:
            return fcf / shares_outstanding

        return None

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
        model.fund_family = (
            self._safe_get(info, "fundFamily", "")
            if model.quote_type.lower() in ["etf", "mutualfund"] else None
        )

        return model

    def extract_fundamentals(self):
        info = self._get_info()
        financials = self._get_financials()
        balance_sheet = self._get_balance_sheet()
        cash_flow = self._get_cash_flow()
        quote_type = self._safe_get(info, "quoteType", "")
        model = FundamentalsModel(ticker=self.ticker)
        model.currency = self._safe_get(info, "currency", "")
        model.day = OHLCModel(
            low=self._safe_float(
                self._safe_get(info, "dayLow", self._get_price_at_period(period="1d", parameter="low"))
            ),
            high=self._safe_float(
                self._safe_get(info, "dayHigh", self._get_price_at_period(period="1d", parameter="high"))
            ),
            open=self._safe_float(
                self._safe_get(info, "open", self._get_price_at_period(period="1d", parameter="open"))
            ),
            close=self._safe_float(
                self._safe_get(info, "currentPrice", self._get_price_at_period(period="1d", parameter="close"))
            )
        )
        model.one_year = OHLCModel(
            low=self._safe_float(
                self._safe_get(info, "fiftyTwoWeekLow", self._get_price_at_period(period="1y", parameter="low"))
            ),
            high=self._safe_float(
                self._safe_get(info, "fiftyTwoWeekHigh", self._get_price_at_period(period="1y", parameter="high"))
            ),
            open=self._get_price_at_period(period="1y", parameter="open"),
            close=self._get_price_at_period(period="1y", parameter="close")
        )
        model.trailing_pe = (
            self._safe_float(self._safe_get(info, "trailingPE", ""))
            if quote_type.lower() not in ["etf", "mutualfund"] else None
        )
        model.forward_pe = (
            self._safe_float(self._safe_get(info, "forwardPE", ""))
            if quote_type.lower() not in ["etf", "mutualfund"] else None
        )
        model.trailing_eps = (
            self._safe_float(self._safe_get(info, "trailingEps", ""))
            if quote_type.lower() not in ["etf", "mutualfund"] else None
        )
        model.forward_eps = (
            self._safe_float(self._safe_get(info, "forwardEps", ""))
            if quote_type.lower() not in ["etf", "mutualfund"] else None
        )
        model.nav_price = (
            self._safe_float(self._safe_get(info, "navPrice", ""))
            if quote_type.lower() in ["etf", "mutualfund"] else None
        )
        model.dist_yield = (
            self._safe_float(self._safe_get(info, "yield", ""))
            if quote_type.lower() in ["etf", "mutualfund"] else None
        )
        model.expense_ratio = (
            self._safe_float(self._safe_get(info, "expenseRatio", ""))
            if quote_type.lower() in ["etf", "mutualfund"] else None
        )
        model.peg = self._get_peg(info=info, fundamentals=model)
        model.price_to_book = self._get_price_to_book(info=info)
        model.price_to_sales = self._get_price_to_sales(info=info)
        model.ev_ebitda = self._get_ev_ebitda(info=info)
        model.gross_margin = self._get_gross_margin(info=info, financials=financials)
        model.operating_margin = self._get_operating_margin(info=info, financials=financials)
        model.profit_margin = self._get_profit_margin(info=info, financials=financials)
        model.return_on_equity = self._get_return_on_equity(info=info, financials=financials,
                                                            balance_sheet=balance_sheet)
        model.return_on_assets = self._get_return_on_assets(info=info, financials=financials,
                                                            balance_sheet=balance_sheet)
        model.debt_to_equity = self._get_debt_to_equity(info=info, balance_sheet=balance_sheet)
        model.free_cash_flow_per_share = self._get_free_cash_flow_per_share(info=info, cash_flow=cash_flow)

        return model
