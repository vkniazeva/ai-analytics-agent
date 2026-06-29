import pytest
import pandas as pd
import numpy as np

from forecasting.data_preparation.data_cleanup.data_cleaning import clean_data
from forecasting.utils.exceptions import ValidationError


# Helper function to create valid dataframe
def create_valid_dataframe():
    return pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc3", "abc4"],
        "item_id": ["AB123", "AB124", "AB125", "AB126"],
        "potential_error": [pd.NA, pd.NA, pd.NA, pd.NA],
        "sold_quantity": [10, 15, 20, 25],
        "origin": ["city_001", "city_002", "city_001", "city_003"]
    })


# Happy path - clean dataset with no errors
def test_clean_data_success():
    df = create_valid_dataframe()
    original_rows = len(df)

    cleaned_df = clean_data(df)

    # All rows should remain
    assert len(cleaned_df) == original_rows
    # potential_error column should be removed (fully NA)
    assert "potential_error" not in cleaned_df.columns
    # Other columns should remain
    assert "flight_key" in cleaned_df.columns
    assert "item_id" in cleaned_df.columns


# Test potential_error removal - errors present
def test_clean_data_with_potential_errors():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc3", "abc4"],
        "item_id": ["AB123", "AB124", "AB125", "AB126"],
        "potential_error": [pd.NA, "error_1", pd.NA, "error_2"],
        "sold_quantity": [10, 15, 20, 25]
    })

    cleaned_df = clean_data(df)

    # Two rows with errors should be removed
    assert len(cleaned_df) == 2
    # Only abc1 and abc3 should remain
    assert set(cleaned_df["flight_key"]) == {"abc1", "abc3"}
    # potential_error column should be removed
    assert "potential_error" not in cleaned_df.columns


def test_clean_data_all_rows_have_errors():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2"],
        "item_id": ["AB123", "AB124"],
        "potential_error": ["error_1", "error_2"],
        "sold_quantity": [10, 15]
    })

    # All rows would be removed, resulting in empty dataframe
    with pytest.raises(ValidationError, match="Dataset is empty after cleaning"):
        clean_data(df)


# Test duplicate removal
def test_clean_data_with_duplicates():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc2", "abc3"],
        "item_id": ["AB123", "AB124", "AB124", "AB125"],
        "potential_error": [pd.NA, pd.NA, pd.NA, pd.NA],
        "sold_quantity": [10, 15, 16, 20]  # Different values for duplicate
    })

    cleaned_df = clean_data(df)

    # One duplicate should be removed (keeps first occurrence)
    assert len(cleaned_df) == 3
    # Check grain uniqueness
    assert cleaned_df.duplicated(subset=["flight_key", "item_id"]).sum() == 0
    # First occurrence should be kept (sold_quantity=15, not 16)
    abc2_row = cleaned_df[cleaned_df["flight_key"] == "abc2"]
    assert abc2_row["sold_quantity"].iloc[0] == 15


def test_clean_data_multiple_duplicates():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc1", "abc1", "abc2"],
        "item_id": ["AB123", "AB123", "AB123", "AB124"],
        "potential_error": [pd.NA, pd.NA, pd.NA, pd.NA],
        "sold_quantity": [10, 11, 12, 20]
    })

    cleaned_df = clean_data(df)

    # Only 2 unique records should remain
    assert len(cleaned_df) == 2
    assert cleaned_df.duplicated(subset=["flight_key", "item_id"]).sum() == 0


# Test NaN handling
def test_clean_data_removes_fully_empty_columns():
    df = create_valid_dataframe()
    # Add fully empty column
    df["empty_column"] = pd.NA

    cleaned_df = clean_data(df)

    # Empty column should be removed
    assert "empty_column" not in cleaned_df.columns


def test_clean_data_removes_fully_empty_rows():
    df = create_valid_dataframe()
    # Add fully empty row
    df.loc[len(df)] = [pd.NA] * len(df.columns)

    cleaned_df = clean_data(df)

    # Empty row should be removed
    assert len(cleaned_df) == 4  # Original 4 rows, not 5


def test_clean_data_partial_nan_values():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc3"],
        "item_id": ["AB123", "AB124", pd.NA],  # Partial NaN
        "potential_error": [pd.NA, pd.NA, pd.NA],
        "sold_quantity": [10, 15, 20]
    })

    cleaned_df = clean_data(df)

    # Row with any NaN should be removed (dropna with how='all' only removes fully empty)
    # Actually dropna(how='all') keeps rows with partial NaN
    # This is expected behavior - validation should catch this
    assert len(cleaned_df) == 3


# Integration test - multiple cleaning operations
def test_clean_data_combined_issues():
    df = pd.DataFrame({
        "flight_key": ["abc1", "abc2", "abc2", "abc3", "abc4"],
        "item_id": ["AB123", "AB124", "AB124", "AB125", "AB126"],
        "potential_error": [pd.NA, "error_1", pd.NA, pd.NA, pd.NA],
        "sold_quantity": [10, 15, 16, 20, 25],
        "empty_col": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA]
    })

    cleaned_df = clean_data(df)

    # Step 1: abc2 first occurrence removed due to error -> 4 rows remain
    # Step 2: After error removal, no duplicates exist -> 4 rows remain
    # Step 3: empty_col and potential_error removed
    # Result: abc1, abc2 (second occurrence), abc3, abc4
    assert len(cleaned_df) == 4
    assert set(cleaned_df["flight_key"]) == {"abc1", "abc2", "abc3", "abc4"}
    assert "empty_col" not in cleaned_df.columns
    assert "potential_error" not in cleaned_df.columns


# Edge case - empty dataframe
def test_clean_data_empty_dataframe():
    df = pd.DataFrame({
        "flight_key": pd.Series([], dtype=str),
        "item_id": pd.Series([], dtype=str),
        "potential_error": pd.Series([], dtype=object),
        "sold_quantity": pd.Series([], dtype=int)
    })

    # Empty dataframe should raise ValidationError
    with pytest.raises(ValidationError, match="Dataset is empty after cleaning"):
        clean_data(df)