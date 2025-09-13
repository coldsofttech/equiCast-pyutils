import json
from dataclasses import asdict, dataclass

import pandas as pd


@dataclass
class ExportableModel:
    """Base class to provide JSON and Parquet export capabilities."""

    @property
    def is_empty(self) -> bool:
        """Check if the model is empty (must be implemented by subclass)."""
        raise NotImplementedError("Subclassess must implement is_empty.")

    def to_json(self, filepath: str = None, indent: int = 4) -> str:
        """Export object to JSON string or file."""
        data = asdict(self)
        json_str = json.dumps(data, indent=indent, default=str)

        if filepath:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(json_str)
        return json_str

    def to_parquet(self, filepath: str):
        """Export object to Parquet file."""
        df = self._to_dataframe()
        df.to_parquet(filepath, index=False)

    def _to_dataframe(self) -> pd.DataFrame:
        """Convert object to a DataFrame (must be implemented by subclass)."""
        raise NotImplementedError("Subclasses must implement _to_dataframe().")
