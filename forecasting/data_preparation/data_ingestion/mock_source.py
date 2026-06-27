import pandas as pd
from pathlib import Path
from forecasting.data_preparation.data_ingestion.base import BaseSource

class MockSource(BaseSource):

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def fetch(self) -> pd.DataFrame:
        if not self.path.exists():
            raise FileNotFoundError(f"Mock file not found: {self.path}")
        return pd.read_parquet(self.path)