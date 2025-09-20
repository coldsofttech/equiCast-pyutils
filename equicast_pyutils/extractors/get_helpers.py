import random
import time
from dataclasses import dataclass

from equicast_pyutils.extractors.retry import retry


@dataclass
class GetHelpers:
    @staticmethod
    @retry(delay=2)
    def get_history(yf_obj, interval="1d", period=None, start=None, end=None):
        time.sleep(random.uniform(0.1, 0.5))
        if period:
            data = yf_obj.history(period=period, interval=interval)
        else:
            data = yf_obj.history(start=start, end=end, interval=interval)

        fallback_periods = ["20y", "15y", "10y", "5y", "1y"]
        for fallback in fallback_periods:
            if data.empty:
                data = yf_obj.history(period=fallback, interval=interval)
                print(f"⏳ No data found. Fallback to {fallback}.")
            else:
                break

        if data.empty:
            raise ValueError("No historical data found for the specified ticker.")

        return data

    @staticmethod
    @retry(delay=2)
    def get_info(yf_obj):
        time.sleep(random.uniform(0.1, 0.5))
        info = yf_obj.info
        if not info or len(info) < 5:
            info = yf_obj.get_info()

        if not info or len(info) < 5:
            raise ValueError("No info found for the specified ticker.")

        return info

    @staticmethod
    def get_price_at_period(yf_obj, period: str = "1d", parameter: str = "close"):
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
        history = GetHelpers.get_history(yf_obj, period=u_period)
        if history.empty:
            return None

        first_row = history.iloc[-1] if period == "1d" else history.iloc[0]
        for col in param_map[parameter]:
            if col in history.columns:
                return float(first_row[col])

        return None

    @staticmethod
    def check_delisted(yf_obj, info=None, history=None):
        try:
            if info is None:
                info = yf_obj.info if yf_obj else {}
            if history is None and yf_obj:
                history = yf_obj.history(period="1y")

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

            return is_delisted
        except Exception:
            return True
