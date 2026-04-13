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


def save_processed(df, filename):
    path = PROCESSED_PATH / filename
    df.to_parquet(path)

def load_payments():
    payments_df = load_file("xlsx", "payment*")
    renamed_cols = {
        "Session No": "session_id",
        "Order No": "load_id",
        "Ticket ID": "slip_id",
        "Flight No": "flight_no",
        "Flight Origin": "origin",
        "Flight Destination": "destination",
        "Offline": "is_offline_mode",
        "Sales Type": "sales_type",
        "Payment Type": "payment_type",
        "Amount Tendered": "purchase_amount",
        "Card Digits": "card_number_prefix",
        "Card Type": "card_type",
    }
    payments_df = rename_cols(renamed_cols, payments_df)
    payments_df["session_id"] = payments_df["session_id"].str[7:]
    payments_df["card_number_prefix"] = payments_df["card_number_prefix"].str[:6]
    payments_df = process_flight_data(payments_df)
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
    pax_df = rename_cols(renamed_cols, pax_df)
    pax_df = process_flight_data(pax_df)
    pax_df["scheduled_date"] = pd.to_datetime(pax_df["scheduled_date"], format="%d/%m/%y")
    print(pax_df.head(5))
    return pax_df

def process_flight_data(df):
    df["flight_no"] = df["flight_no"].astype(str)
    df["flight_no"] = "AB" + df["flight_no"].str[2:]
    cities_map = load_city_mapping(CONFIG_PATH)
    df["origin"] = df["origin"].map(cities_map).fillna("UNKNOWN")
    df["destination"] = df["destination"].map(cities_map).fillna("UNKNOWN")
    return df


def rename_cols(renamed_cols:dict, df:DataFrame):
    columns_to_leave = renamed_cols.values()
    df = df.rename(columns=renamed_cols)[columns_to_leave]
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