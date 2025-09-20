import json
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from typing import Dict

import pandas as pd

from equicast_pyutils.models import ExportableModel, OHLCModel


@dataclass
class FundamentalsModel(ExportableModel):
    ticker: str
    currency: str = field(default=None, init=False)
    day: OHLCModel = field(default=None, init=False)
    one_year: OHLCModel = field(default=None, init=False)
    trailing_pe: float = field(default=None, init=False)
    forward_pe: float = field(default=None, init=False)
    trailing_eps: float = field(default=None, init=False)
    forward_eps: float = field(default=None, init=False)
    nav_price: float = field(default=None, init=False)
    dist_yield: float = field(default=None, init=False)
    expense_ratio: float = field(default=None, init=False)
    peg: float = field(default=None, init=False)
    price_to_book: float = field(default=None, init=False)
    price_to_sales: float = field(default=None, init=False)
    ev_ebitda: float = field(default=None, init=False)
    gross_margin: float = field(default=None, init=False)
    operating_margin: float = field(default=None, init=False)
    profit_margin: float = field(default=None, init=False)
    return_on_equity: float = field(default=None, init=False)
    return_on_assets: float = field(default=None, init=False)
    debt_to_equity: float = field(default=None, init=False)
    free_cash_flow_per_share: float = field(default=None, init=False)
    metadata: Dict[str, str] = field(
        default_factory=lambda: {"lastUpdated": datetime.now().isoformat()}
    )

    @property
    def empty(self) -> bool:
        """Check if the model is empty."""
        return False if self.currency else True

    def _flatten_dataclass(self, d):
        """Recursively flatten nested dataclasses into a single dictionary."""
        result = {}
        for k, v in d.items():
            if is_dataclass(v):
                nested = self._flatten_dataclass(asdict(v))
                # include only non-empty nested fields
                for nk, nv in nested.items():
                    if nv not in (None, "", {}):
                        result[f"{k}_{nk}"] = nv
            elif isinstance(v, dict):
                # flatten dicts, skip empty values
                for nk, nv in v.items():
                    if nv not in (None, "", {}):
                        result[f"{k}_{nk}"] = nv
            else:
                if v not in (None, "", {}):
                    result[k] = v
        return result

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert fundamentals into a pandas DataFrame for export."""
        data = asdict(self)
        data["metadata"] = json.dumps(data["metadata"])
        flat_data = self._flatten_dataclass(data)
        df = pd.DataFrame([flat_data])
        return df

    def to_parquet(self, filepath: str):
        """Export fundamentals to a parquet file."""
        df = self._to_dataframe()
        if not df.empty:
            df.to_parquet(filepath, index=False)
