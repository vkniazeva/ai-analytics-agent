import pytest
import pandas as pd
import numpy as np

from forecasting.data_preparation.data_validation.data_validating import (
    validate_data,
    _check_columns,
    _check_nan_values,
    _check_target,
    _check_hist_avg,
    _check_dates
)
from forecasting.utils.exceptions import ValidationError


# Helper function to create valid dataframe
def create_valid_dataframe():
    return pd.DataFrame({
        "flight_key": ["f1", "f2", "f3", "f4", "f5"],
        "origin": ["city_001"] * 5,
        "destination": ["city_003"] * 5,
        "date": pd.to_datetime(["2025-11-15", "2025-12-01", "2026-01-10", "2026-02-05", "2026-02-20"]),
        "item_id": ["AB123"] * 5,
        "sold_quantity": [10, 15, 20, 25, 30],
        "number_of_passengers": [89, 90, 91, 92, 93],
        "is_night": [False] * 5,
        "hist_avg": [12.5, 13.0, 18.5, 22.0, 27.5],
        "hist_level_used": [1.0, 2.0, 3.0, 4.0, 1.0]
    })


# Happy path - all validations pass
def test_validate_data_success():
    df = create_valid_dataframe()
    # Should not raise any exception
    validate_data(df)


# Test _check_columns
def test_check_columns_missing_columns():
    df = pd.DataFrame({
        "flight_key": ["f1"],
        "origin": ["city_001"],
        # missing other required columns
    })
    required_columns = ["flight_key", "origin", "destination", "date", "item_id"]

    with pytest.raises(ValidationError, match="Missing columns"):
        _check_columns(df, required_columns)


def test_check_columns_success():
    df = create_valid_dataframe()
    required_columns = ["flight_key", "origin", "destination", "date", "item_id"]
    # Should not raise
    _check_columns(df, required_columns)


# Test _check_nan_values
def test_check_nan_values_with_nans():
    df = create_valid_dataframe()
    df.loc[0, "item_id"] = None
    df.loc[2, "sold_quantity"] = np.nan

    required_columns = ["item_id", "sold_quantity", "origin"]

    with pytest.raises(ValidationError, match="NaN values are present in"):
        _check_nan_values(df, required_columns)


def test_check_nan_values_success():
    df = create_valid_dataframe()
    required_columns = ["item_id", "sold_quantity", "origin"]
    # Should not raise
    _check_nan_values(df, required_columns)


# Test _check_target
def test_check_target_not_numeric():
    df = create_valid_dataframe()
    df["sold_quantity"] = df["sold_quantity"].astype(str)

    with pytest.raises(ValidationError, match="is not numeric"):
        _check_target(df, "sold_quantity")


def test_check_target_negative_values():
    df = create_valid_dataframe()
    df.loc[0, "sold_quantity"] = -5

    with pytest.raises(ValidationError, match="has negative values"):
        _check_target(df, "sold_quantity")


def test_check_target_success():
    df = create_valid_dataframe()
    # Should not raise
    _check_target(df, "sold_quantity")


def test_check_target_zero_allowed():
    df = create_valid_dataframe()
    df.loc[0, "sold_quantity"] = 0
    # Zero should be allowed (not negative)
    _check_target(df, "sold_quantity")


# Test _check_hist_avg
def test_check_hist_avg_with_nan():
    df = create_valid_dataframe()
    df.loc[0, "hist_avg"] = np.nan

    with pytest.raises(ValidationError, match="hist_avg column contains NaN"):
        _check_hist_avg(df, hist_avg_levels=4)


def test_check_hist_avg_invalid_level():
    df = create_valid_dataframe()
    df.loc[0, "hist_level_used"] = 5.0  # Invalid (only 1-4 allowed)

    with pytest.raises(ValidationError, match="Invalid values in hist_level_used"):
        _check_hist_avg(df, hist_avg_levels=4)


def test_check_hist_avg_success():
    df = create_valid_dataframe()
    # Should not raise
    _check_hist_avg(df, hist_avg_levels=4)


# Test _check_dates
def test_check_dates_start_before_range():
    df = create_valid_dataframe()
    df.loc[0, "date"] = pd.to_datetime("2025-10-15")  # Before start_date

    with pytest.raises(ValidationError, match="Dataset start date .* is outside expected range"):
        _check_dates(df, start_date="2025-11-01", end_date="2026-02-28")


def test_check_dates_end_after_range():
    df = create_valid_dataframe()
    df.loc[4, "date"] = pd.to_datetime("2026-03-15")  # After end_date

    with pytest.raises(ValidationError, match="Dataset end date .* is outside expected range"):
        _check_dates(df, start_date="2025-11-01", end_date="2026-02-28")


def test_check_dates_success():
    df = create_valid_dataframe()
    # Should not raise
    _check_dates(df, start_date="2025-11-01", end_date="2026-02-28")


def test_check_dates_edge_cases_exact_boundaries():
    df = pd.DataFrame({
        "date": pd.to_datetime(["2025-11-01", "2026-02-28"])
    })
    # Exact boundaries should be valid
    _check_dates(df, start_date="2025-11-01", end_date="2026-02-28")


# Integration test - multiple validation failures
def test_validate_data_multiple_failures():
    df = pd.DataFrame({
        "flight_key": ["f1", "f2"],
        "origin": ["city_001", None],  # NaN value
        "destination": ["city_003", "city_004"],
        "date": pd.to_datetime(["2025-11-15", "2026-05-01"]),  # Second date out of range
        "item_id": ["AB123", "AB124"],
        "sold_quantity": [10, -5],  # Negative value
        "number_of_passengers": [89, 90],
        "is_night": [False, True],
        "hist_avg": [12.5, 13.0],
        "hist_level_used": [1.0, 2.0]
    })

    # Should fail on one of the validations
    with pytest.raises(ValidationError):
        validate_data(df)

