from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from equicast_pyutils.models import ExportableModel


@dataclass
class MetadataModel(ExportableModel):
    last_updated: Optional[datetime] = field(default_factory=lambda: datetime.now(timezone.utc))
    source: Optional[str] = None

    def _to_dataframe(self) -> pd.DataFrame:
        data = asdict(self)
        if data['last_updated']:
            data['last_updated'] = data['last_updated'].isoformat()
        df = pd.DataFrame([data])
        return df
