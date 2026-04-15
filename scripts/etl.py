"""
ETL (Extract, Transform, Load) module for data processing tasks.
- loads data from data/raw
- cleaning columns
- anonymizing data
- fixing data types
- saves processed data to data/processed - as parquet

"""
from pathlib import Path
import pandas as pd
import json

from pandas.core.interchange.dataframe_protocol import DataFrame

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data/raw"
PROCESSED_PATH = BASE_DIR / "data/processed"
CONFIG_PATH = BASE_DIR / "data/config/cities_mapping.json"
pd.set_option('display.max_columns', None)

def main():
    pax_df = load_pax()
    save_processed(pax_df, "pax.parquet")
    payment_df = load_payments()
    save_processed(payment_df, "payments.parquet")
    sales_df = load_sales()
    save_processed(sales_df, "sales.parquet")


def save_processed(df, filename):
    path = PROCESSED_PATH / filename
    df.to_parquet(path)

def load_sales():
    sales_df = load_file("xlsx", "sales*")
    renamed_cols = {
        "Session No": "session_id",
        "Order No": "load_id",
        "Flight No": "flight_no",
        "Flight Origin": "origin",
        "Flight Destination": "destination",
        "Scheduled Date": "scheduled_date",
        "Ticket ID": "slip_id",
        "Sales Type": "sales_type",
        "Item Category": "item_category",
        "Item Reference": "item_id",
        "Item Price": "price",
        "Qty Sold": "quantity",
        "Sale Amount": "purchase_amount",
        "Promotion Discount": "discount_amount",
    }
    sales_df = rename_cols(sales_df, renamed_cols)
    schema = {
        "session_id": "string",
        "load_id": "string",
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "scheduled_date": "date",
        "slip_id": "string",
        "sales_type": "string",
        "item_category": "string",
        "item_id": "string",
        "price": "float",
        "quantity": "int",
        "purchase_amount": "float",
        "discount_amount": "float"
    }
    sales_df["session_id"] = sales_df["session_id"].str[7:]
    sales_df = process_flight_data(sales_df)
    sales_df = format_cols(sales_df, schema, date_separator="T")
    print(sales_df.head(5))
    return sales_df

def load_payments():
    payments_df = load_file("xlsx", "payment*")
    renamed_cols = {
        "Session No": "session_id",
        "Order No": "load_id",
        "Ticket ID": "slip_id",
        "Flight No": "flight_no",
        "Flight Origin": "origin",
        "Flight Destination": "destination",
        "Transaction Time": "transaction_time",
        "Offline": "is_offline_mode",
        "Sales Type": "sales_type",
        "Payment Type": "payment_type",
        "Amount Tendered": "purchase_amount",
        "Card Digits": "card_number_prefix",
        "Card Type": "card_type",
    }
    payments_df = rename_cols(payments_df, renamed_cols)
    schema = {
        "session_id": "string",
        "load_id": "string",
        "slip_id": "string",
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "transaction_time": "date",
        "is_offline_mode": "string",
        "sales_type": "string",
        "payment_type": "string",
        "purchase_amount": "float",
        "card_number_prefix": "string",
        "card_type": "string",
    }
    payments_df = format_cols(payments_df, schema, date_separator="T")
    payments_df["session_id"] = payments_df["session_id"].str[7:]
    payments_df["card_number_prefix"] = payments_df["card_number_prefix"].str[:6]
    payments_df = process_flight_data(payments_df)
    print(payments_df.head(5))
    return payments_df


def load_pax():
    pax_df = load_file("csv", "pax*")
    renamed_cols = {
        "Flight Number": "flight_no",
        "Scheduled Date": "scheduled_date",
        "Scheduled Time": "scheduled_time",
        "Origin": "origin",
        "Destination": "destination",
        "Class": "class",
        "PAX": "pax"
    }
    pax_df = rename_cols(pax_df, renamed_cols)
    schema = {
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "class": "string",
        "pax": "string"
    }
    pax_df = process_flight_data(pax_df)
    pax_df = format_cols(pax_df, schema)
    pax_df["date"] = pd.to_datetime(pax_df["scheduled_date"], format="%d/%m/%y")
    print(pax_df.head(5))
    return pax_df

def process_flight_data(df):
    df["flight_no"] = df["flight_no"].astype(str)
    df["flight_no"] = "AB" + df["flight_no"].str[2:]
    cities_map = load_city_mapping(CONFIG_PATH)
    df["origin"] = df["origin"].map(cities_map).fillna("UNKNOWN")
    df["destination"] = df["destination"].map(cities_map).fillna("UNKNOWN")
    return df


def rename_cols(df:DataFrame, renamed_cols:dict):
    columns_to_leave = renamed_cols.values()
    df = df.rename(columns=renamed_cols)[columns_to_leave]
    return df


def format_cols(df:DataFrame, schema:dict, date_separator=None):
    for column, dtype in schema.items():
        if (dtype == "date") & (date_separator is not None) :
            df["date"] = pd.to_datetime(df[column].str.split(date_separator).str[0], format="%Y-%m-%d")
            df["time"] = pd.to_datetime(df[column].str.split(date_separator).str[1], format="%H:%M:%S").dt.time
            df = df.drop(column, axis=1)
        else: df[column] = df[column].astype(dtype)
    return df


def load_city_mapping(path):
    """here cities_mapping.json file is used to load the data,
    but for privacy reasons it is excluded from the available files on github,
    instead mapping_example.json can be used"""
    with open(path, "r") as f:
        return json.load(f)


def load_file(type:str, prefix:str):
    path = Path(RAW_PATH)
    files = list(path.glob(prefix))
    dfs = []
    for file in files:
        if type == "csv":
            df = pd.read_csv(file, delimiter=";")
        else:
            df = pd.read_excel(file)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    return df


if __name__ == "__main__":
    main()