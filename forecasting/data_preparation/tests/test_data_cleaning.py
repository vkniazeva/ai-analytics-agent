import pandas as pd
from forecasting.data_preparation.data_cleanup.data_cleaning import clean_data
import pytest

# happy pass, no errors in the dataset
# expected potential_error column should be removed, number of records should be the same
def test_clean_dataset():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc3"],
        "item_id": ["AB123", "AB124", "AB125"],
        "potential_error": [pd.NA, pd.NA, pd.NA],
        "some_column": ["random_1", "random_2", pd.NA]
    })

    cleaned_df = clean_data(df)

    assert len(df) == len(cleaned_df)
    assert len(df.columns)-1 == len(cleaned_df.columns)
    assert "potential_error" not in cleaned_df.columns


# potential error found
# expected potential_error column should be removed, record with error is removed
def test_potential_error_is_given():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc3"],
        "item_id": ["AB123", "AB124", "AB125"],
        "potential_error": [pd.NA, pd.NA, "error1"],
        "some_column": ["random_1", "random_2", pd.NA]
    })

    cleaned_df = clean_data(df)

    assert len(df)-1 == len(cleaned_df)
    assert len(df.columns)-1 == len(cleaned_df.columns)
    assert "abc3" not in cleaned_df["flight_key"].values
    assert "potential_error" not in cleaned_df.columns


# duplicate by grain is found
# expected potential_error column should be removed, record with duplicate is removed
def test_duplicate_is_given():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc2"],
        "item_id": ["AB123", "AB124", "AB124"],
        "potential_error": [pd.NA, pd.NA, pd.NA],
        "some_column": ["random_1", "random_2", "random_3"]
    })

    cleaned_df = clean_data(df)

    assert len(df)-1 == len(cleaned_df)
    assert len(df.columns)-1 == len(cleaned_df.columns)
    assert "random_3" not in cleaned_df["some_column"].values
    assert "potential_error" not in cleaned_df.columns