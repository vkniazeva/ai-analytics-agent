from forecasting.data_preparation.data_ingestion import MockSource, DBSource
import yaml
from pathlib import Path


forecasting_path = Path(__file__).parent
config_path =  forecasting_path / "configs" / "config.yaml"

with open(config_path, "r") as f:
    config = yaml.safe_load(f)

data_source_type = config["data_preparation"]["data_ingestion"]["data_source"]

if data_source_type == "mock":
    source = MockSource(path=forecasting_path/"interim_files/raw_data_df.parquet")
else:
    source = DBSource()

df = source.fetch()
print(f"STEP 1: DATA IS FETCHD FROM THE SOURCE {data_source_type}")

