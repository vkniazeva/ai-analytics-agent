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

PK_MAP = {
    "dim_product": "product_sur_id",
    "dim_flight": "flight_sur_id",
    "dim_date": "date_sur_id",
    "dim_load": "load_sur_id",
    "dim_card": "card_sur_key",
    "dim_session": "session_sur_id",
    "fact_pax": "pax_sur_id",
    "fact_payment": "payment_sur_id",
    "fact_sales": "sales_sur_id",
    "fact_wastage": "wastage_sur_id",
}

FK_MAP = {
    "flight_key": ("dim_flight", "flight_sur_id"),
    "product_key": ("dim_product", "product_sur_id"),
    "item_key": ("dim_product", "product_sur_id"),
    "session_key": ("dim_session", "session_sur_id"),
    "card_key": ("dim_card", "card_sur_key"),
    "date_key": ("dim_date", "date_sur_id"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_all():
    conn = get_connection()
    conn.autocommit = False
    try:
        for table in TABLES:
            load_table(conn, table)
        cur = conn.cursor()
        for table in TABLES:
            df = pd.read_parquet(DWH_PATH / f"{table}.parquet")
            add_foreign_keys(cur, table, df)
        cur.close()
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

    pk_col = PK_MAP.get(table_name)
    if pk_col:
        columns.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(pk_col)))

    create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(columns)
    )
    cur.execute(create_stmt)


def add_foreign_keys(cur, table_name: str, df: pd.DataFrame):
    for col_name in df.columns:
        if col_name not in FK_MAP:
            continue
        ref_table, ref_col = FK_MAP[col_name]
        cur.execute(sql.SQL(
            "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})"
        ).format(
            sql.Identifier(table_name),
            sql.Identifier(f"fk_{table_name}_{col_name}"),
            sql.Identifier(col_name),
            sql.Identifier(ref_table),
            sql.Identifier(ref_col),
        ))
        cur.execute(sql.SQL(
            "CREATE INDEX {} ON {} ({})"
        ).format(
            sql.Identifier(f"idx_{table_name}_{col_name}"),
            sql.Identifier(table_name),
            sql.Identifier(col_name),
        ))


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


MART_INDEXES = {
    "mart_sales_performance": ["date", "month"],
    "mart_product_sales": ["item_id", "item_category"],
    "mart_flight_sales": ["flight_no", "route", "date"],
}

MART_VIEWS = {
    "mart_sales_performance": """
        SELECT
            d.date,
            d.month,
            d.is_weekend,
            SUM(f.purchase_amount) AS total_sales,
            COUNT(DISTINCT f.slip_id) AS total_transactions,
            COUNT(f.product_key) AS total_items,
            AVG(f.purchase_amount) AS avg_item_price,
            SUM(f.purchase_amount) / NULLIF(COUNT(DISTINCT f.slip_id), 0) AS avg_check
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_sur_id
        GROUP BY d.date, d.month, d.is_weekend
    """,
    "mart_product_sales": """
        SELECT
            p.item_id,
            p.item_category,
            p.item_type,
            p.is_food,
            SUM(f.purchase_amount) AS total_sales,
            COUNT(f.product_key) AS quantity_sold,
            COUNT(DISTINCT f.slip_id) AS total_transactions,
            COUNT(f.product_key) * 1.0 / NULLIF(COUNT(DISTINCT f.slip_id), 0) AS attach_rate
        FROM fact_sales f
        JOIN dim_product p ON f.product_key = p.product_sur_id
        GROUP BY p.item_id, p.item_category, p.item_type, p.is_food
    """,
    "mart_flight_sales": """
        SELECT
            fl.flight_no,
            fl.origin || '-' || fl.destination AS route,
            d.date,
            d.is_weekend,
            SUM(f.purchase_amount) AS total_sales,
            MAX(pax.total_pax) AS total_pax,
            SUM(f.purchase_amount) / NULLIF(MAX(pax.total_pax), 0) AS revenue_per_pax
        FROM fact_sales f
        JOIN dim_flight fl ON f.flight_key = fl.flight_sur_id
        JOIN dim_date d ON f.date_key = d.date_sur_id
        LEFT JOIN (
            SELECT flight_key, SUM(pax_quantity) AS total_pax
            FROM fact_pax
            GROUP BY flight_key
        ) pax ON f.flight_key = pax.flight_key
        GROUP BY fl.flight_no, fl.origin, fl.destination, d.date, d.is_weekend
    """,
}


def create_marts():
    conn = get_connection()
    conn.autocommit = False
    try:
        cur = conn.cursor()
        for mart_name, query in MART_VIEWS.items():
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(mart_name)))
            cur.execute(sql.SQL("CREATE TABLE {} AS {}").format(
                sql.Identifier(mart_name),
                sql.SQL(query)
            ))
            for col in MART_INDEXES.get(mart_name, []):
                cur.execute(sql.SQL("CREATE INDEX {} ON {} ({})").format(
                    sql.Identifier(f"idx_{mart_name}_{col}"),
                    sql.Identifier(mart_name),
                    sql.Identifier(col),
                ))
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(mart_name)))
            count = cur.fetchone()[0]
            print(f"  {mart_name}: {count} rows")
        cur.close()
        conn.commit()
        print("All marts created successfully.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def map_dtype(dtype, col_name: str) -> str:
    dtype_str = str(dtype)
    if col_name.endswith("_key") or col_name.endswith("_sur_id"):
        return "VARCHAR(32)"
    if any(x in col_name for x in ["price", "amount"]):
        return "NUMERIC(12,2)"
    if "quantity" in col_name:
        return "INTEGER"
    if "int" in dtype_str:
        return "INTEGER"
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
