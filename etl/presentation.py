import pandas as pd

from etl.staging import BASE_DIR

DWH_PATH = BASE_DIR / "data/dwh"
MARTS_PATH = BASE_DIR / "data/marts"


def main():
    mart_sales_performance()
    mart_product_sales()
    mart_flight_sales()


def read_dwh(name: str):
    return pd.read_parquet(DWH_PATH / f"{name}.parquet")


def save_mart(df, name: str):
    df.to_parquet(MARTS_PATH / f"{name}.parquet", index=False)
    print(f"  {name}: {len(df)} rows")


def mart_sales_performance():
    sales = read_dwh("fact_sales")
    dim_date = read_dwh("dim_date")

    df = sales.merge(dim_date, left_on="date_key", right_on="date_sur_id")

    mart = df.groupby(["date", "month", "is_weekend"]).agg(
        total_sales=("purchase_amount", "sum"),
        total_transactions=("slip_id", "nunique"),
        total_items=("product_key", "count"),
    ).reset_index()

    mart["avg_item_price"] = mart["total_sales"] / mart["total_items"]
    mart["avg_check"] = mart["total_sales"] / mart["total_transactions"]

    save_mart(mart, "mart_sales_performance")


def mart_product_sales():
    sales = read_dwh("fact_sales")
    dim_product = read_dwh("dim_product")

    df = sales.merge(dim_product, left_on="product_key", right_on="product_sur_id")

    mart = df.groupby(["item_id", "item_category", "item_type", "is_food"]).agg(
        total_sales=("purchase_amount", "sum"),
        quantity_sold=("product_key", "count"),
        total_transactions=("slip_id", "nunique"),
    ).reset_index()

    mart["attach_rate"] = mart["quantity_sold"] / mart["total_transactions"]

    save_mart(mart, "mart_product_sales")


def mart_flight_sales():
    sales = read_dwh("fact_sales")
    pax = read_dwh("fact_pax")
    dim_flight = read_dwh("dim_flight")
    dim_date = read_dwh("dim_date")

    dim_flight["route"] = dim_flight["origin"] + "-" + dim_flight["destination"]

    df = sales.merge(dim_flight[["flight_sur_id", "flight_no", "route"]], left_on="flight_key", right_on="flight_sur_id")
    df = df.merge(dim_date[["date_sur_id", "date", "is_weekend"]], left_on="date_key", right_on="date_sur_id")

    pax_agg = pax.groupby("flight_key").agg(total_pax=("pax_quantity", "sum")).reset_index()
    df = df.merge(pax_agg, on="flight_key", how="left")

    mart = df.groupby(["flight_no", "route", "date", "is_weekend"]).agg(
        total_sales=("purchase_amount", "sum"),
        total_pax=("total_pax", "first"),
    ).reset_index()

    mart["revenue_per_pax"] = mart["total_sales"] / mart["total_pax"].replace(0, pd.NA)

    save_mart(mart, "mart_flight_sales")


if __name__ == "__main__":
    main()
