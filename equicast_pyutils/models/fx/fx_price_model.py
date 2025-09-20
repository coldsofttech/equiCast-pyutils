import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import pandas as pd

from equicast_pyutils.models import ExportableModel, OHLCModel, MetadataModel


@dataclass
class FxPriceModel(ExportableModel):
    from_currency: str
    to_currency: str
    prices: List[OHLCModel] = field(default_factory=list)
    metadata: MetadataModel = field(default_factory=MetadataModel)

    @property
    def pair(self) -> str:
        return f"{self.from_currency}{self.to_currency}"

    def empty(self) -> bool:
        return not bool(self.prices)

    def _to_dataframe(self) -> pd.DataFrame:
        if self.empty():
            return pd.DataFrame()

        rows = []
        for ohlc in self.prices:
            row = {
                'from': self.from_currency,
                'to': self.to_currency,
                'date': ohlc.date.isoformat() if ohlc.date else None,
                'open': round(ohlc.open, 6),
                'high': round(ohlc.high, 6),
                'low': round(ohlc.low, 6),
                'close': round(ohlc.close, 6),
                'average': ohlc.average,
                'lastUpdated': self.metadata.last_updated,
                'source': self.metadata.source
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        return df

    def to_parquet(self, filename: str, base_folder: str):
        df = self._to_dataframe()
        if df.empty:
            return

        os.makedirs(base_folder, exist_ok=True)
        df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
        for year, group in df.groupby(df['date'].dt.year):
            year_folder = Path(base_folder) / f"fx={self.pair}" / f"year={year}"
            year_folder.mkdir(parents=True, exist_ok=True)
            file_path = year_folder / filename
            group.to_parquet(file_path, index=False, engine="pyarrow")
