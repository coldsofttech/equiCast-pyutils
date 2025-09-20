import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from equicast_pyutils.models import ExportableModel, MetadataModel


@dataclass
class FxCalculationModel(ExportableModel):
    from_currency: str
    to_currency: str
    volatility: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    cagr_1y: Optional[float] = None
    cagr_5y: Optional[float] = None
    metadata: MetadataModel = field(default_factory=MetadataModel)

    @property
    def pair(self) -> str:
        return f"{self.from_currency}{self.to_currency}"

    def empty(self) -> bool:
        return not bool(self.volatility or self.sharpe_ratio or self.max_drawdown)

    def _to_dataframe(self) -> pd.DataFrame:
        if self.empty():
            return pd.DataFrame()

        row = {
            'from': self.from_currency,
            'to': self.to_currency,
            'volatility': round(self.volatility, 6),
            'sharpeRatio': round(self.sharpe_ratio, 6),
            'maxDrawdown': round(self.max_drawdown, 6),
            'cagr1Y': round(self.cagr_1y, 6),
            'cagr5Y': round(self.cagr_5y, 6),
            'lastUpdated': self.metadata.last_updated,
            'source': self.metadata.source
        }

        df = pd.DataFrame([row])
        return df

    def to_parquet(self, filename: str, base_folder: str):
        df = self._to_dataframe()
        if df.empty:
            return

        os.makedirs(base_folder, exist_ok=True)
        file_path = Path(base_folder) / f"fx={self.pair}" / filename
        df.to_parquet(file_path, index=False, engine="pyarrow")
