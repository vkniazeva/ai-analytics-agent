"""
Raw data loader: reads CSV/Excel from data/raw/ and loads into PostgreSQL `raw` schema.

No transformations — just faithful transfer of source files into the database.
dbt handles all transformations downstream.
"""
import io
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# --- CONFIG ---

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data" / "raw"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

SCHEMA = "raw"

# Each dataset: name -> (file_glob, file_type, read_kwargs, columns_to_drop)
DATASETS = {
    "pax": {
        "glob": "pax*.csv",
        "type": "csv",
        "read_kwargs": {"delimiter": ";"},
        "drop_columns": [],
    },
    "sales": {
        "glob": "sales*.xlsx",
        "type": "xlsx",
        "read_kwargs": {},
        "drop_columns": ["Staff ID", "Staff Name"],
    },
    "payments": {
        "glob": "payment*.xlsx",
        "type": "xlsx",
        "read_kwargs": {},
        "drop_columns": [],
    },
    "wastage": {
        "glob": "wastage*.xlsx",
        "type": "xlsx",
        "read_kwargs": {},
        "drop_columns": [],
    },
    "schedule": {
        "glob": "schedule*.csv",
        "type": "csv",
        "read_kwargs": {"delimiter": ","},
        "drop_columns": [],
    },
    "product_catalog": {
        "glob": "product_catalog*.xlsx",
        "type": "xlsx",
        "read_kwargs": {},
        "drop_columns": [],
    },
    "bank": {
        "glob": "bank*.csv",
        "type": "csv",
        "read_kwargs": {"delimiter": ","},
        "drop_columns": [],
    },
    "orders": {
        "glob": "order_summary*.csv",
        "type": "csv",
        "read_kwargs": {"delimiter": ","},
        "drop_columns": [],
    },
    "line_load": {
        "glob": "lines*.csv",
        "type": "csv",
        "read_kwargs": {"delimiter": ","},
        "drop_columns": [],
    }
}


# --- MAIN ---

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        for dataset_name, config in DATASETS.items():
            load_dataset(cur, dataset_name, config)
        conn.commit()
        print("All raw tables loaded successfully.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def load_dataset(cur, dataset_name: str, config: dict):
    df = read_files(config)
    df = drop_prohibited_columns(df, config["drop_columns"])
    table_name = f"{SCHEMA}.{dataset_name}"
    create_table(cur, table_name, df)
    copy_data(cur, table_name, df)
    print(f"  {table_name}: {len(df)} rows loaded ({len(df.columns)} columns)")


# --- READ ---

def read_files(config: dict) -> pd.DataFrame:
    files = sorted(RAW_PATH.glob(config["glob"]))
    if not files:
        raise FileNotFoundError(f"No files matching {config['glob']} in {RAW_PATH}")

    dfs = []
    for file in files:
        if config["type"] == "csv":
            df = pd.read_csv(file, **config["read_kwargs"])
        else:
            df = pd.read_excel(file, **config["read_kwargs"])
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def drop_prohibited_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    existing = [col for col in columns if col in df.columns]
    if existing:
        df = df.drop(columns=existing)
    return df


# --- LOAD TO POSTGRES ---

def create_table(cur, table_name: str, df: pd.DataFrame):
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

    columns_sql = []
    for col_name, dtype in df.dtypes.items():
        pg_type = pandas_dtype_to_pg(dtype)
        safe_col = f'"{col_name}"'
        columns_sql.append(f"{safe_col} {pg_type}")

    columns_def = ", ".join(columns_sql)
    cur.execute(f"CREATE TABLE {table_name} ({columns_def})")


def copy_data(cur, table_name: str, df: pd.DataFrame):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    columns = ", ".join(f'"{col}"' for col in df.columns)
    copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT text, NULL '\\N')"
    cur.copy_expert(copy_sql, buffer)


def pandas_dtype_to_pg(dtype) -> str:
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return "BIGINT"
    elif "float" in dtype_str:
        return "DOUBLE PRECISION"
    elif "bool" in dtype_str:
        return "BOOLEAN"
    elif "datetime" in dtype_str:
        return "TIMESTAMP"
    else:
        return "TEXT"


if __name__ == "__main__":
    main()
