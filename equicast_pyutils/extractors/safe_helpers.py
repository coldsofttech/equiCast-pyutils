import math
from dataclasses import dataclass


@dataclass
class SafeHelpers:
    @staticmethod
    def safe_get(info, key, default=None):
        try:
            return info.get(key, default)
        except Exception:
            return default

    @staticmethod
    def safe_float(val, default: float = 0.0):
        try:
            val = float(val)
            if math.isnan(val) or math.isinf(val):
                return default
            return val
        except Exception:
            return default
