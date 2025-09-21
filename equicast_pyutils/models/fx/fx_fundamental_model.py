import os
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from equicast_pyutils.models import ExportableModel, OHLCModel, MetadataModel


@dataclass
class FxFundamentalModel(ExportableModel):
    from_currency: str
    to_currency: str
    ma50: float
    ma200: float
    day: OHLCModel = field(default_factory=OHLCModel)
    year: OHLCModel = field(default_factory=OHLCModel)
    metadata: MetadataModel = field(default_factory=MetadataModel)

    @property
    def pair(self) -> str:
        return f"{self.from_currency}{self.to_currency}"

    def empty(self) -> bool:
        return not bool(self.day and self.year)

    def _to_dataframe(self) -> pd.DataFrame:
        if self.empty():
            return pd.DataFrame()

        row = {
            'from': self.from_currency,
            'to': self.to_currency,
            'dayOpen': round(self.day.open, 6),
            'dayHigh': round(self.day.high, 6),
            'dayLow': round(self.day.low, 6),
            'dayClose': round(self.day.close, 6),
            'dayAverage': round(self.day.average, 6),
            'yearOpen': round(self.year.open, 6),
            'yearHigh': round(self.year.high, 6),
            'yearLow': round(self.year.low, 6),
            'yearClose': round(self.year.close, 6),
            'yearAverage': round(self.year.average, 6),
            'movingAverage50Days': round(self.ma50, 6),
            'movingAverage200Days': round(self.ma200, 6),
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
        fx_folder = Path(base_folder) / f"fx={self.pair}"
        fx_folder.mkdir(parents=True, exist_ok=True)
        file_path = fx_folder / filename
        df.to_parquet(file_path, index=False, engine="pyarrow")
