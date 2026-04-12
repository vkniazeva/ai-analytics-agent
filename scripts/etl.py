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

BASE_DIR = Path(__file__).resolve().parent.parent

RAW_PATH = BASE_DIR / "data/raw"
PROCESSED_PATH = BASE_DIR / "data/processed"
CONFIG_PATH = BASE_DIR / "data/config/cities_mapping.json"

def main():
    pax_df = load_pax()
    save_processed(pax_df, "pax.parquet")


def save_processed(df, filename):
    path = PROCESSED_PATH / filename
    df.to_parquet(path)

def load_pax():
    path = Path(RAW_PATH)
    files = list(path.glob("pax*"))
    dfs = []
    for file in files:
        df = pd.read_csv(file, delimiter=";")
        dfs.append(df)
    renamed_cols = {
        "Flight Number": "flight_no",
        "Scheduled Date": "scheduled_date",
        "Scheduled Time": "scheduled_time",
        "Origin": "origin",
        "Destination": "destination",
        "Class": "class",
        "PAX": "pax"
    }
    columns_to_leave = ["flight_no", "scheduled_date", "scheduled_time", "origin", "destination", "class", "pax"]
    pax_df = pd.concat(dfs, ignore_index=True)
    pax_df = pax_df.rename(columns = renamed_cols)[columns_to_leave]
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


def load_city_mapping(path):
    """here cities_mapping.json file is used to load the data,
    but for privacy reasons it is excluded from the available files on github,
    instead mapping_example.json can be used"""
    with open(path, "r") as f:
        return json.load(f)


if __name__ == "__main__":
    main()