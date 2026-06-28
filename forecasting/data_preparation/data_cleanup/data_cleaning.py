import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:

    print(f"Dataset shape before cleanup: {df.shape}")

    # Removing potential errors
    potential_errors = df[df["potential_error"].notna()]
    if len(potential_errors) > 0:
        print(f"Number of found errors: {len(potential_errors)}")
        print(potential_errors["potential_error"].value_counts())
    else:
        print("No errors found")
    df = df[df["potential_error"].isna()]

    # Removing duplicates on grain: flight_key × product_key
    duplicates = df.duplicated(subset=["flight_key", "item_id"]).sum()
    print(f"Number of found duplicates: {duplicates}")
    df = df.drop_duplicates(subset=["flight_key", "item_id"])

    # Dropping fully empty columns and rows with any NaN
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all")

    print(f"Dataset shape after cleanup: {df.shape}")
    return df