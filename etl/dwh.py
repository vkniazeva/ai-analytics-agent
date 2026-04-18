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

pd.set_option("display.max_columns", None)

def main():
    dim_product()


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
    pass

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