import numpy as np
import pandas as pd

from forecasting.utils.config_handler import return_config
from forecasting.utils.database import write_sql


def _create_pax_bins(df: pd.DataFrame, pax_bins_config: list) -> pd.DataFrame:

    df["pax_bin"] = pd.cut(df["number_of_passengers"],
                            bins=pax_bins_config,
                            labels=["<100", "100 - 150", "150 - 180", "180 +"], include_lowest=True)
    return df


def _create_route(df: pd.DataFrame) -> pd.DataFrame:
    df["route"] = df["origin"] + " _ " + df["destination"]
    return df

def _create_hist_avg(df: pd.DataFrame, min_samples: int) -> pd.DataFrame:
    # Level 1
    df = df.copy()

    hist_avg_l1 = df.groupby(["item_id", "route", "day_period"]).agg({
        "sold_quantity" : ["mean", "count"]
    })
    hist_avg_l1.columns = ["hist_avg_l1", "hist_count_l1"]
    hist_avg_l1 = hist_avg_l1.reset_index()

    # Level 2
    hist_avg_l2 = df.groupby(["item_id", "route", "is_night"]).agg({
        "sold_quantity": ["mean", "count"]
    })
    hist_avg_l2.columns = ["hist_avg_l2", "hist_count_l2"]
    hist_avg_l2 = hist_avg_l2.reset_index()

    # Level 3
    hist_avg_l3 = df.groupby(["item_id", "day_period"]).agg({
        "sold_quantity": ["mean", "count"]
    })
    hist_avg_l3.columns = ["hist_avg_l3", "hist_count_l3"]
    hist_avg_l3 = hist_avg_l3.reset_index()

    # Level 4
    hist_avg_l4 = df.groupby(["item_id"]).agg({
        "sold_quantity": ["mean", "count"]
    })
    hist_avg_l4.columns = ["hist_avg_l4", "hist_count_l4"]
    hist_avg_l4 = hist_avg_l4.reset_index()

    # merging columns
    df = df.merge(hist_avg_l1, on=["item_id", "route", "day_period"], how="left")
    df = df.merge(hist_avg_l2, on=["item_id", "route", "is_night"], how="left")
    df = df.merge(hist_avg_l3, on=["item_id", "day_period"], how="left")
    df = df.merge(hist_avg_l4, on=["item_id"], how="left")

    # putting avg
    df["hist_avg"] = np.nan
    df["hist_level_used"] = np.nan

    mask_l1 = (df["hist_count_l1"] >= min_samples)
    df.loc[mask_l1, "hist_avg"] = df.loc[mask_l1, "hist_avg_l1"]
    df.loc[mask_l1, "hist_level_used"] = 1

    mask_l2 = df["hist_avg"].isna() & (df["hist_count_l2"] >= min_samples)
    df.loc[mask_l2, "hist_avg"] = df.loc[mask_l2, "hist_avg_l2"]
    df.loc[mask_l2, "hist_level_used"] = 2

    mask_l3 = df["hist_avg"].isna() & (df["hist_count_l3"] >= min_samples)
    df.loc[mask_l3, "hist_avg"] = df.loc[mask_l3, "hist_avg_l3"]
    df.loc[mask_l3, "hist_level_used"] = 3

    # last level doesn't have any limitations as it is a fallback
    mask_l4 = df["hist_avg"].isna() & (df["hist_count_l4"] >= 0)
    df.loc[mask_l4, "hist_avg"] = df.loc[mask_l4, "hist_avg_l4"]
    df.loc[mask_l4, "hist_level_used"] = 4

    cols_to_drop = [c for c in df.columns if c.startswith("hist_avg_l") or c.startswith("hist_count_l")]
    df["hist_avg"] = df["hist_avg"].round(2)
    df = df.drop(cols_to_drop, axis=1)
    return df

def save_hist_avg_lookup(df: pd.DataFrame) -> None:
    lookup = df[["item_id", "route", "day_period", "hist_avg", "hist_level_used"]]
    lookup = lookup.drop_duplicates(subset=["item_id", "route", "day_period"])
    table_name = "lookup_hist_avg"
    write_sql(lookup, table_name)

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    config = return_config()
    features_config = config["data_preparation"]["feature_engineering"]
    df = _create_pax_bins(df, features_config["pax_bins"])
    df = _create_route(df)
    df = _create_hist_avg(df, features_config["min_samples"])
    # save hist_avg lookup for inference
    save_hist_avg_lookup(df)
    return df