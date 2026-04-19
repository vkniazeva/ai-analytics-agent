"""
Data Warehouse (DWH) module for ETL pipeline.
- takes the data from the parquet files (processed)
- maps into dims and facts
- stores as parquet files in dwh
"""
from pathlib import Path

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

    print(dim_product_df.head(5))
    save_dwh(dim_product_df, "product")

def dim_flights():
    keys = ["flight_no", "date", "time", "origin", "destination"]

    # schedule (data source)
    schedule_df = (read_parquet("schedule")[keys + ["line_id"]].drop_duplicates(subset=keys))
    schedule_df["source"] = "KNOWN_DATA"

    # checking sales, transactions, pax to contain unknown flights
    files = ["sales", "payments", "pax"]
    other_df = pd.concat([read_parquet(f)[keys] for f in files], ignore_index=True).drop_duplicates(subset=keys)
    unknown_df = other_df.merge(schedule_df[keys], on=keys, how="left", indicator=True)
    unknown_df = unknown_df[unknown_df["_merge"] == "left_only"].drop(columns="_merge")
    unknown_df["line_id"] = pd.NA
    unknown_df["source"] = "UNKNOWN"

    #merging data
    dim_flight_df = pd.concat([schedule_df, unknown_df], ignore_index=True)
    dim_flight_df = generate_int_key(dim_flight_df, "flight_id")
    print(dim_flight_df.head(5))
    save_dwh(dim_flight_df, "flight")

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

    print(dim_date_df.head(5))
    save_dwh(dim_date_df, "date")

def dim_load():
    load_df = read_parquet("schedule")[["line_id", "load_id"]].drop_duplicates()
    load_df = load_df.drop_duplicates(subset=["line_id"])
    load_df["load_id"] = load_df["load_id"].astype("string").fillna("UNKNOWN")
    load_df = generate_int_key(load_df, "load_key")
    print(load_df.head(5))
    save_dwh(load_df, "load")

# UTILS
def read_parquet(file: str):
    file_path = PROCESSED_PATH / f"{file}.parquet"
    return pd.read_parquet(file_path)

def save_dwh(df, file: str):
    file_path = DWH_PATH / f"dim_{file}.parquet"
    df.to_parquet(file_path)

def generate_int_key(df, column_name: str):
    df = df.reset_index(drop=True)
    df[column_name] = df.index + 1
    return df



if __name__ == "__main__":
    main()