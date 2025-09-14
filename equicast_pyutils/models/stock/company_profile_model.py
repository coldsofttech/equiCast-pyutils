import json
from dataclasses import dataclass, field, asdict, is_dataclass
from datetime import datetime
from typing import Dict, List

import pandas as pd

from equicast_pyutils.models.base import ExportableModel


@dataclass
class CompanyAddressModel(ExportableModel):
    address1: str = field(default=None, init=False)
    address2: str = field(default=None, init=False)
    city: str = field(default=None, init=False)
    state: str = field(default=None, init=False)
    zip: str = field(default=None, init=False)
    country: str = field(default=None, init=False)
    region: str = field(default=None, init=False)

    @property
    def is_empty(self) -> bool:
        """Check if the model is empty."""
        return False if self.country else True

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert company address into a pandas DataFrame for export."""
        data = asdict(self)
        df = pd.DataFrame([data])
        return df

    def to_parquet(self, filepath: str):
        """Export company address to a parquet file."""
        df = self._to_dataframe()
        if not df.empty:
            df.to_parquet(filepath, index=False)


@dataclass
class CompanyOfficerModel(ExportableModel):
    name: str = field(default=None, init=False)
    title: str = field(default=None, init=False)

    @property
    def is_empty(self) -> bool:
        """Check if the model is empty."""
        return False if self.name else True

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert company officer into a pandas DataFrame for export."""
        data = asdict(self)
        df = pd.DataFrame([data])
        return df

    def to_parquet(self, filepath: str):
        """Export company officer to a parquet file."""
        df = self._to_dataframe()
        if not df.empty:
            df.to_parquet(filepath, index=False)


@dataclass
class CompanyProfileModel(ExportableModel):
    ticker: str
    name: str = field(default=None, init=False)
    quote_type: str = field(default=None, init=False)
    exchange: str = field(default=None, init=False)
    currency: str = field(default=None, init=False)
    description: str = field(default=None, init=False)
    sector: str = field(default=None, init=False)
    industry: str = field(default=None, init=False)
    website: str = field(default=None, init=False)
    beta: float = field(default=None, init=False)
    payout_ratio: float = field(default=None, init=False)
    dividend_rate: float = field(default=None, init=False)
    dividend_yield: float = field(default=None, init=False)
    market_cap: int = field(default=None, init=False)
    volume: int = field(default=None, init=False)
    address: CompanyAddressModel = field(default=None, init=False)
    full_time_employees: int = field(default=None, init=False)
    ceos: List[CompanyOfficerModel] = field(default=None, init=False)
    ipo_date: datetime = field(default=None, init=False)
    fund_family: str = field(default=None, init=False)
    metadata: Dict[str, str] = field(
        default_factory=lambda: {"lastUpdated": datetime.now().isoformat()}
    )

    @property
    def is_empty(self) -> bool:
        """Check if the model is empty."""
        return False if self.exchange else True

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
        """Convert company profile into a pandas DataFrame for export."""
        data = asdict(self)
        data["metadata"] = json.dumps(data["metadata"])
        data["ceos"] = json.dumps(data["ceos"])
        if data.get("ipo_date"):
            data["ipo_date"] = data["ipo_date"].isoformat()
        flat_data = self._flatten_dataclass(data)
        df = pd.DataFrame([flat_data])
        return df

    def to_parquet(self, filepath: str):
        """Export company profile to a parquet file."""
        df = self._to_dataframe()
        if not df.empty:
            df.to_parquet(filepath, index=False)
