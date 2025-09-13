from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

import pandas as pd

from equicast_pyutils.models.base import ExportableModel


@dataclass
class FxConversionRateModel(ExportableModel):
    pair: str
    from_currency: str
    to_currency: str
    rates: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, str] = field(
        default_factory=lambda: {"lastUpdated": datetime.now().isoformat()}
    )

    @property
    def is_empty(self) -> bool:
        """Check if the model is empty."""
        return len(self.rates) == 0

    def add_rate(self, date: str, rate: float):
        """Add or update a conversion rate for a given date."""
        try:
            datetime.strptime(date, "%Y-%m-%d")  # Enforce ISO date format
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format.")
        self.rates[date] = rate
        self.metadata["lastUpdated"] = datetime.now().isoformat()

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert FX rates into a pandas DataFrame for export."""
        rows = [{"date": d, "rate": r} for d, r in self.rates.items()]
        df = pd.DataFrame(rows)
        df.insert(0, "pair", self.pair)
        df.insert(1, "from", self.from_currency)
        df.insert(2, "to", self.to_currency)
        return df

    def to_parquet(self, filepath: str):
        """Export FX rates to a parquet file."""
        df = self._to_dataframe()
        if not df.empty:
            df.to_parquet(filepath, index=False)
