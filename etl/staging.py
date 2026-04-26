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


# CONFIG / PATHS
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "data/raw"
PROCESSED_PATH = BASE_DIR / "data/processed"
CONFIG_PATH = BASE_DIR / "data/config/cities_mapping.json"

START_DATE = pd.to_datetime("2026-01-01").date()
END_DATE = pd.to_datetime("2026-03-31").date()

pd.set_option('display.max_columns', None)


def main():
    run_pipeline("pax")
    run_pipeline("sales")
    run_pipeline("payments")
    run_pipeline("wastage")
    run_pipeline("schedule")
    run_pipeline("product_catalog")

# PIPELINES
def run_pipeline(dataset_name: str):
    df = load(dataset_name)
    df = standardize(df, dataset_name)
    df = clean(df, dataset_name)
    save(df, dataset_name)

# LOAD LAYER
def load(dataset_name: str):
    if dataset_name == "pax":
        return load_pax()
    elif dataset_name == "sales":
        return load_sales()
    elif dataset_name == "payments":
        return load_payments()
    elif dataset_name == "wastage":
        return load_wastage()
    elif dataset_name == "schedule":
        return load_schedule()
    elif dataset_name == "product_catalog":
        return load_product_catalog()
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")

def load_pax():
    return load_file("csv", "pax*")

def load_sales():
    return load_file("xlsx", "sales*")

def load_payments():
    return load_file("xlsx", "payment*")

def load_wastage():
    return load_file("xlsx", "wastage*")

def load_schedule():
    return load_file("csv", "schedule*")

def load_product_catalog():
    return load_file("xlsx", "product_catalog*")

# STANDARDIZATION LAYER
def standardize(df, dataset_name: str):
    if dataset_name == "pax":
        return standardize_pax(df)
    elif dataset_name == "sales":
        return standardize_sales(df)
    elif dataset_name == "payments":
        return standardize_payments(df)
    elif dataset_name == "wastage":
        return standardize_wastage(df)
    elif dataset_name == "schedule":
        return standardize_schedule(df)
    elif dataset_name == "product_catalog":
        return standardize_product_catalog(df)
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")

def standardize_pax(df):
    renamed_cols = {
        "Flight Number": "flight_no",
        "Scheduled Date": "date",
        "Scheduled Time": "time",
        "Origin": "origin",
        "Destination": "destination",
        "Class": "class",
        "PAX": "pax_qty"
    }
    schema = {
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "class": "string",
        "pax_qty": "int"
    }
    df = rename_cols(df, renamed_cols)
    df = process_flight_data(df)
    df = format_cols(df, schema)
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%y").dt.date
    df["time"] = pd.to_datetime(df["time"], format="%H:%M").dt.time
    return df

def standardize_sales(df):
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
    df = rename_cols(df, renamed_cols)
    df["session_id"] = df["session_id"].str[7:]
    df = process_flight_data(df)
    df = format_cols(df, schema, date_separator="T", date_format="%Y-%m-%d", time_format="%H:%M:%S")
    return df

def standardize_payments(df):
    renamed_cols = {
        "Session No": "session_id",
        "Order No": "load_id",
        "Ticket ID": "slip_id",
        "Flight No": "flight_no",
        "Flight Origin": "origin",
        "Flight Destination": "destination",
        "Scheduled Date": "scheduled_date",
        "Offline": "is_offline_mode",
        "Sales Type": "sales_type",
        "Payment Type": "payment_type",
        "Amount Tendered": "purchase_amount",
        "Card Digits": "card_number_prefix",
        "Card Type": "card_type",
    }
    schema = {
        "session_id": "string",
        "load_id": "string",
        "slip_id": "string",
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "scheduled_date": "date",
        "is_offline_mode": "boolean",
        "sales_type": "string",
        "payment_type": "string",
        "purchase_amount": "float",
        "card_number_prefix": "string",
        "card_type": "string",
    }
    df = rename_cols(df, renamed_cols)
    df = format_cols(df, schema, date_separator="T", date_format="%Y-%m-%d", time_format="%H:%M:%S")
    df["session_id"] = df["session_id"].str[7:]
    df["card_number_prefix"] = df["card_number_prefix"].str[:6]
    df = process_flight_data(df)
    return df

def standardize_wastage(df):
    renamed_cols = {
        "Order No": "load_id",
        "Flight No": "flight_no",
        "Scheduled Route": "route",
        "Scheduled Date": "scheduled_date",
        "Item Category": "item_category",
        "Item Reference": "item_id",
        "Item Type": "item_type",
        "Ordered Qty": "load_quantity",
        "Sold Qty": "sold_quantity",
        "Damaged Waste Qty": "wastage_quantity",
        "QTY Fresh Waste": "fresh_wastage_quantity"
    }
    schema = {
        "load_id": "string",
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "item_category": "string",
        "item_id": "string",
        "item_type": "string",
        "load_quantity": "int64",
        "sold_quantity": "int64",
        "wastage_quantity": "int64",
        "fresh_wastage_quantity": "int64"
    }
    df = rename_cols(df, renamed_cols)
    df["origin"] = df["route"].str.split("-").str[0]
    df["destination"] = df["route"].str.split("-").str[1]
    df = df.drop("route", axis=1)
    df = process_flight_data(df)
    df["date"] = pd.to_datetime(df["scheduled_date"], format="%d-%m-%Y").dt.date
    df = format_cols(df, schema)
    return df

def standardize_schedule(df):
    renamed_cols = {
        "line_id": "line_id",
        "flight_no": "flight_no",
        "iata_departure": "origin",
        "iata_destination": "destination",
        "scheduled_datetime": "scheduled_date",
        "order_no": "load_id"
    }
    schema = {
        "line_id": "string",
        "flight_no": "string",
        "origin": "string",
        "destination": "string",
        "scheduled_date": "date",
        "load_id": "Int64"
    }
    df = rename_cols(df, renamed_cols)
    df = process_flight_data(df)
    df = format_cols(df, schema, date_separator=" ", date_format="%d/%m/%y", time_format="%H:%M")
    return df

def standardize_product_catalog(df):
    renamed_cols = {
        "Reference": "item_id",
        "Status": "status",
        "Family": "item_category",
        "Food": "is_food",
        "Type": "item_type",
        "Selling Price": "price"
    }
    schema = {
        "item_id": "string",
        "status": "string",
        "item_category": "string",
        "is_food": "boolean",
        "item_type": "string",
        "price": "float"
    }
    df = rename_cols(df, renamed_cols)
    boolean_map = {
        "Yes": True,
        "No": False
    }
    df["is_food"] = df["is_food"].map(boolean_map)
    df = format_cols(df, schema)
    return df

# CLEAN
def clean(df, dataset_name: str):
    if dataset_name == "pax":
        return clean_pax(df)
    elif dataset_name == "sales":
        return clean_sales(df)
    elif dataset_name == "payments":
        return clean_payments(df)
    elif dataset_name == "wastage":
        return clean_wastage(df)
    elif dataset_name == "schedule":
        return clean_schedule(df)
    elif dataset_name == "product_catalog":
        return clean_product_catalog(df)
    else:
        raise ValueError(f"Unknown dataset: {dataset_name}")

def clean_pax(df):
    df = drop_duplicates(df)
    required_cols = ["flight_no", "date", "pax_qty"]
    df = drop_invalid_nan(df, required_cols)
    not_negative_cols = ["pax_qty"]
    df = filter_negatives(df, not_negative_cols)
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)]
    return df

def clean_sales(df):
    df = drop_duplicates(df)
    required_cols = ["session_id", "load_id", "flight_no", "date", "slip_id", "sales_type", "item_id", "quantity", "purchase_amount"]
    df = drop_invalid_nan(df, required_cols)
    not_negative_cols = ["quantity", "price", "discount_amount"]
    df = filter_negatives(df, not_negative_cols)
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)]
    return df

def clean_payments(df):
    df = drop_duplicates(df)
    required_cols = ["session_id", "load_id", "slip_id", "flight_no", "sales_type", "payment_type", "purchase_amount"]
    df = drop_invalid_nan(df, required_cols)
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)]
    return df

def clean_wastage(df):
    df = drop_duplicates(df)
    required_cols = ["load_id", "flight_no", "date", "item_id", "load_quantity", "sold_quantity"]
    df = drop_invalid_nan(df, required_cols)
    not_negative_cols = ["load_quantity", "sold_quantity", "wastage_quantity", "fresh_wastage_quantity"]
    df = filter_negatives(df, not_negative_cols)
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)]
    return df

def clean_schedule(df):
    df = drop_duplicates(df)
    required_cols = ["line_id", "flight_no", "date"]
    df = drop_invalid_nan(df, required_cols)
    df = df[(df["date"] >= START_DATE) & (df["date"] <= END_DATE)]
    return df

def clean_product_catalog(df):
    df = drop_duplicates(df)
    required_cols = ["item_id", "item_category", "price"]
    df = drop_invalid_nan(df, required_cols)
    return df


# SAVE
def save(df, dataset_name: str):
    print(dataset_name.upper())
    print(df.head(5))
    path = PROCESSED_PATH / f"{dataset_name}.parquet"
    df.to_parquet(path)

# UTILITIES
def rename_cols(df, renamed_cols: dict):
    columns_to_leave = renamed_cols.values()
    df = df.rename(columns=renamed_cols)[columns_to_leave]
    return df

def format_cols(df, schema: dict, date_separator=None, date_format=None, time_format=None):
    for column, dtype in schema.items():
        if (dtype == "date") and (date_separator is not None):
            df["date"] = pd.to_datetime(df[column].str.split(date_separator).str[0], format=date_format).dt.date
            df["time"] = pd.to_datetime(df[column].str.split(date_separator).str[1], format=time_format).dt.time
            df = df.drop(column, axis=1)
        else:
            df[column] = df[column].astype(dtype)
    return df

def process_flight_data(df):
    df["flight_no"] = df["flight_no"].astype(str)
    df["flight_no"] = "AB" + df["flight_no"].str[2:]
    cities_map = load_city_mapping(CONFIG_PATH)
    df["origin"] = df["origin"].map(cities_map).fillna("UNKNOWN")
    df["destination"] = df["destination"].map(cities_map).fillna("UNKNOWN")
    return df

def load_city_mapping(path):
    with open(path, "r") as f:
        return json.load(f)

def load_file(type: str, prefix: str):
    files = list(RAW_PATH.glob(prefix))
    dfs = []
    for file in files:
        if type == "csv":
            df = pd.read_csv(file, delimiter=";")
        else:
            df = pd.read_excel(file)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def drop_duplicates(df):
    df = df.drop_duplicates()
    return df

def drop_invalid_nan(df, required_cols: list):
    df = df.replace("nan", pd.NA)
    df = df.dropna(subset=required_cols)
    return df

def filter_negatives(df, cols):
    for col in cols:
        df = df[df[col] >= 0]
    return df

if __name__ == "__main__":
    main()

