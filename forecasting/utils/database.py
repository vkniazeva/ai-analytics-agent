import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from forecasting.utils.config_handler import FORECASTING_PATH, return_config

load_dotenv()

def get_engine():
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5433')
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

def read_sql(query: str, table_name: str = None) -> pd.DataFrame:
    config = return_config()
    if config["data_preparation"]["data_ingestion"]["data_source"] == "mock":
        path = FORECASTING_PATH / "interim_files" / f"{table_name}.csv"
        return pd.read_csv(path)
    else:
        return pd.read_sql(query, get_engine())

def write_sql(df: pd.DataFrame, table_name: str, schema: str = "forecasting") -> None:
    config = return_config()
    if config["data_preparation"]["data_ingestion"]["data_source"] == "mock":
        path = FORECASTING_PATH / "interim_files" / f"{table_name}.csv"
        df.to_csv(path)
    else:
        # Use replace for lookup tables to avoid duplicates
        if_exists_mode = "replace" if table_name.startswith("lookup_") else "append"
        df.to_sql(
            name=table_name,
            con=get_engine(),
            schema=schema,
            if_exists=if_exists_mode,
            index=False
        )