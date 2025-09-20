import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

from equicast_pyutils.models import ExportableModel, MetadataModel


@dataclass
class FxProfileModel(ExportableModel):
    from_currency: str
    to_currency: str
    exchange: str
    region: str
    quote_type: str
    description: Optional[str] = None
    metadata: MetadataModel = field(default_factory=MetadataModel)

    @property
    def pair(self) -> str:
        return f"{self.from_currency}{self.to_currency}"

    def empty(self) -> bool:
        return not bool(self.exchange and self.region)

    def _to_dataframe(self) -> pd.DataFrame:
        if self.empty():
            return pd.DataFrame()

        row = {
            'from': self.from_currency,
            'to': self.to_currency,
            'exchange': self.exchange,
            'region': self.region,
            'quoteType': self.quote_type,
            'description': self.description,
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
