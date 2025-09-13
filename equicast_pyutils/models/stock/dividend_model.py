from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

import pandas as pd

from equicast_pyutils.models.base import ExportableModel


@dataclass
class DividendModel(ExportableModel):
    ticker: str
    currency: str = field(default_factory=str)
    prices: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, str] = field(
        default_factory=lambda: {"lastUpdated": datetime.now().isoformat()}
    )

    @property
    def is_empty(self) -> bool:
        """Check if the model is empty."""
        return len(self.prices) == 0

    def add_price(self, date: str, rate: float):
        """Add or update a stock price for a given date."""
        try:
            datetime.strptime(date, "%Y-%m-%d")  # Enforce ISO date format
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format.")
        self.prices[date] = rate
        self.metadata["lastUpdated"] = datetime.now().isoformat()

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert stock prices into a pandas DataFrame for export."""
        rows = [{"date": d, "price": p} for d, p in self.prices.items()]
        df = pd.DataFrame(rows)
        df.insert(0, "ticker", self.ticker)
        df.insert(1, "currency", self.currency)
        return df

    def to_parquet(self, filepath: str):
        """Export dividends to a parquet file."""
        df = self._to_dataframe()
        if not df.empty:
            df.to_parquet(filepath, index=False)
