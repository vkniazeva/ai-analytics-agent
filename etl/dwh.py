"""
Data Warehouse (DWH) module for ETL pipeline.
- takes the data from the parquet files (processed)
- maps into dims and facts
- stores as parquet files in dwh
"""
from pathlib import Path
from pickle import FALSE
from traceback import print_tb

import pandas as pd

from etl.staging import BASE_DIR, PROCESSED_PATH

# CONFIG / PATH
DWH_PATH = BASE_DIR / "data/dwh"
INITIAL_DATE = pd.to_datetime("2025-01-01").date()
FINAL_DATE = pd.to_datetime("2027-12-31").date()

pd.set_option("display.max_columns", None)

def main():
    dim_product()
    dim_flights()
    dim_date()
    dim_load()
    dim_card()
    dim_session()

    fact_pax()
    fact_payment()


# DIMS
def dim_product():
    # load data from catalog
    catalog = (read_parquet("product_catalog")[["item_id", "status", "item_category", "is_food", "item_type"]]
               .drop_duplicates(subset=["item_id"]))

    #adding products present in sales but missing in catalog
    sales_products = read_parquet("sales")[["item_id", "item_category"]].drop_duplicates(subset=["item_id"])

    missing_product = sales_products[~sales_products["item_id"].isin(set(catalog["item_id"]))]
    dim_product_df = pd.concat([catalog, missing_product], ignore_index=True).drop_duplicates(subset=["item_id"])
    dim_product_df = generate_int_key(dim_product_df, "product_id")

    save_dwh(dim_product_df, "product", "dim")

def dim_flights():
    keys = ["flight_no", "date", "time", "origin", "destination"]

    schedule_df = (read_parquet("schedule")[keys + ["line_id"]].drop_duplicates(subset=keys))
    schedule_df["source"] = "KNOWN_DATA"

    files = ["sales", "payments", "pax"]
    other_df = pd.concat([read_parquet(f)[keys] for f in files], ignore_index=True).drop_duplicates(subset=keys)
    unknown_df = other_df.merge(schedule_df[keys], on=keys, how="left", indicator=True)
    unknown_df = unknown_df[unknown_df["_merge"] == "left_only"].drop(columns="_merge")
    unknown_df["line_id"] = pd.NA
    unknown_df["source"] = "UNKNOWN"

    dim_flight_df = pd.concat([schedule_df, unknown_df], ignore_index=True)
    dim_flight_df = generate_int_key(dim_flight_df, "flight_id")
    save_dwh(dim_flight_df, "flight", "dim")

def dim_date():
    dates_range = pd.date_range(start=INITIAL_DATE, end=FINAL_DATE, freq="D")
    dim_date_df = pd.DataFrame({"date": dates_range})
    dim_date_df["date_id"] = dim_date_df["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date_df["year"] = dim_date_df["date"].dt.year
    dim_date_df["month"] = dim_date_df["date"].dt.month
    dim_date_df["day"] = dim_date_df["date"].dt.day
    dim_date_df["weekday"] = dim_date_df["date"].dt.weekday
    dim_date_df["weekday_name"] = dim_date_df["date"].dt.day_name()
    dim_date_df["is_weekend"] = dim_date_df["weekday"].isin([5, 6])

    save_dwh(dim_date_df, "date", "dim")

def dim_load():
    load_df = read_parquet("schedule")[["line_id", "load_id"]].drop_duplicates()
    load_df = load_df.drop_duplicates(subset=["line_id"])
    load_df["load_id"] = load_df["load_id"].astype("string").fillna("UNKNOWN")
    load_df = generate_int_key(load_df, "load_key")
    save_dwh(load_df, "load", "dim")

def dim_card():
    card_df = read_parquet("payments")[["card_number_prefix", "card_type"]].drop_duplicates().dropna()
    card_df = card_df.drop_duplicates(subset="card_number_prefix")
    card_df = generate_int_key(card_df, "card_key")
    save_dwh(card_df, "card", "dim")

def dim_session():
    session_df = read_parquet("payments")[["session_id", "is_offline_mode"]].drop_duplicates().drop_duplicates()
    session_df_sales = read_parquet("sales")[["session_id"]].drop_duplicates()
    session_df = pd.concat([session_df, session_df_sales])
    session_df = session_df.drop_duplicates(subset="session_id")
    session_df = generate_int_key(session_df, "session_key")
    session_df = session_df.fillna(False)
    save_dwh(session_df, "session", "dim")

# FACT
def fact_pax():
    pax_df = read_parquet("pax")
    pax_df = map_flight_id(pax_df)
    fact_pax_df = pax_df[["flight_key", "class", "pax_qty"]]
    save_dwh(fact_pax_df, "pax", "fact")

def fact_payment():
    payment_df = read_parquet("payments")
    payment_df = map_flight_id(payment_df)
    payment_df = map_session(payment_df)

    dim_card = read_dim("card")[["card_number_prefix", "card_key"]]
    card_mapping = dim_card.set_index("card_number_prefix")["card_key"]
    payment_df["card_key"] = payment_df["card_number_prefix"].map(card_mapping)
    payment_df = generate_int_key(payment_df, "payment_id")
    fact_payment_df = payment_df[["payment_id", "slip_id", "session_key", "flight_key", "sales_type", "payment_type", "purchase_amount", "card_key"]]

    print(fact_payment_df.head(5))
    save_dwh(fact_payment_df, "payment", "fact")


# UTILS
def read_parquet(file: str):
    file_path = PROCESSED_PATH / f"{file}.parquet"
    return pd.read_parquet(file_path)

def read_dim(file: str):
    file_path = DWH_PATH / f"dim_{file}.parquet"
    return pd.read_parquet(file_path)

def save_dwh(df, file: str, type: str):
    file_path = DWH_PATH / f"{type}_{file}.parquet"
    df.to_parquet(file_path)

def generate_int_key(df, column_name: str):
    df = df.reset_index(drop=True)
    df[column_name] = df.index + 1
    return df

def map_flight_id(df):
    df["flight_key"] = df["flight_no"] + "_" + df["date"].astype(str) + "_" + df[
        "time"].astype(str)
    dim_flight = read_dim("flight")
    dim_flight["flight_key"] = dim_flight["flight_no"] + "_" + dim_flight["date"].astype(str) + "_" + dim_flight[
        "time"].astype(str)
    flight_mapping = dim_flight.set_index("flight_key")["flight_id"]
    df["flight_key"] = df["flight_key"].map(flight_mapping)
    return df

def map_session(df):
    dim_session = read_dim("session")
    session_mapping = dim_session.set_index("session_id")["session_key"]
    df["session_key"] = df["session_id"].map(session_mapping)
    return df


if __name__ == "__main__":
    main()