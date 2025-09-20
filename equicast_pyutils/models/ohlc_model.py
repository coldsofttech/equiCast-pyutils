from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

import pandas as pd

from equicast_pyutils.models import ExportableModel


@dataclass
class OHLCModel(ExportableModel):
    date: Optional[datetime] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    adj_close: Optional[float] = None
    volume: Optional[int] = None

    @property
    def empty(self) -> bool:
        return self.date is None

    @property
    def average(self) -> Optional[float]:
        return round((self.low + self.high) / 2, 6) if self.low is not None and self.high is not None else None

    def _to_dataframe(self) -> pd.DataFrame:
        data = asdict(self)
        if data['date']:
            data['date'] = data['date'].isoformat()
        df = pd.DataFrame([data])
        if self.volume is not None:
            df['volume'] = df['volume'].astype('Int64')
        return df
