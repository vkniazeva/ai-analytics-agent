import io
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

from etl.staging import BASE_DIR

load_dotenv()

DWH_PATH = BASE_DIR / "data/dwh"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

TABLES = [
    "dim_product",
    "dim_flight",
    "dim_date",
    "dim_load",
    "dim_card",
    "dim_session",
    "fact_pax",
    "fact_payment",
    "fact_sales",
    "fact_wastage",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_all():
    conn = get_connection()
    conn.autocommit = False
    try:
        for table in TABLES:
            load_table(conn, table)
        conn.commit()
        print("All tables loaded successfully.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def load_table(conn, table_name: str):
    file_path = DWH_PATH / f"{table_name}.parquet"
    df = pd.read_parquet(file_path)

    cur = conn.cursor()
    create_table(cur, table_name, df)
    copy_data(cur, table_name, df)
    cur.close()

    print(f"  {table_name}: {len(df)} rows loaded")


def create_table(cur, table_name: str, df: pd.DataFrame):
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name)))

    columns = []
    for col_name, dtype in df.dtypes.items():
        pg_type = map_dtype(dtype, col_name)
        columns.append(sql.SQL("{} {}").format(sql.Identifier(col_name), sql.SQL(pg_type)))

    create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns)
    )
    cur.execute(create_stmt)


def copy_data(cur, table_name: str, df: pd.DataFrame):
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    columns = [sql.Identifier(col) for col in df.columns]
    copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT text, NULL '\\N')").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns)
    )
    cur.copy_expert(copy_stmt.as_string(cur), buffer)


def map_dtype(dtype, col_name: str) -> str:
    dtype_str = str(dtype)
    if "int" in dtype_str:
        return "BIGINT"
    elif "float" in dtype_str:
        return "DOUBLE PRECISION"
    elif "bool" in dtype_str:
        return "BOOLEAN"
    elif "datetime" in dtype_str:
        return "TIMESTAMP"
    elif "date" in dtype_str:
        return "DATE"
    else:
        return "TEXT"
