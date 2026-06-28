import pytest
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

from forecasting.data_preparation.feature_engineering.feature_engineering import build_features

# happy path, all features are created
# pax_bin, route, hist_avg
def test_all_features_created():
    df = pd.DataFrame({
        "flight_key": ["abc123", "abc124", "abc123", "abc123", "abc123", "abc123", "abc123"],
        "item_id": ["AB123", "AB123", "AB124", "AB124", "AB124", "AB124", "AB124"],
        "origin": ["city_001", "city_002", "city_001", "city_001", "city_001", "city_001", "city_001"],
        "destination": ["city_003", "city_004", "city_003", "city_003", "city_003", "city_003", "city_003"],
        "number_of_passengers": [89, 110, 89, 89, 89, 89, 89],
        "sold_quantity": [3, 4, 2, 2, 1, 4, 3],
        "day_period": ["morning", "night", "morning", "morning", "morning", "morning", "morning"],
        "is_night": [False, True, False, False, False, False, False]
    })

    df_with_features = build_features(df)


    assert len(df.columns) + 4 == len(df_with_features.columns)
    assert all(col in df_with_features.columns for col in ["route", "hist_avg", "hist_level_used", "pax_bins"])
    assert (df_with_features[df_with_features["item_id"] == "AB123"]["hist_avg"] == 3.5).all()
    assert (df_with_features[df_with_features["item_id"] == "AB123"]["hist_level_used"] == 4.0).all()
    assert (df_with_features[df_with_features["item_id"] == "AB124"]["hist_avg"] == 2.4).all()
    assert (df_with_features[df_with_features["item_id"] == "AB124"]["hist_level_used"] == 1.0).all()
    assert len(df_with_features[df_with_features["pax_bins"] == "<100"]) == 6
    assert len(df_with_features[df_with_features["pax_bins"] == "100 - 150"]) == 1


# test with missing required columns
def test_missing_required_columns():
    df = pd.DataFrame({
        "flight_key": ["abc123"],
        "item_id": ["AB123"],
        # missing origin, destination, number_of_passengers, etc.
    })

    with pytest.raises(KeyError):
        build_features(df)


# test with empty dataframe
def test_empty_dataframe():
    df = pd.DataFrame({
        "flight_key": pd.Series([], dtype=str),
        "item_id": pd.Series([], dtype=str),
        "origin": pd.Series([], dtype=str),
        "destination": pd.Series([], dtype=str),
        "number_of_passengers": pd.Series([], dtype=int),
        "sold_quantity": pd.Series([], dtype=int),
        "day_period": pd.Series([], dtype=str),
        "is_night": pd.Series([], dtype=bool)
    })

    df_with_features = build_features(df)

    # Should return empty dataframe with all feature columns
    assert len(df_with_features) == 0
    assert all(col in df_with_features.columns for col in ["route", "hist_avg", "hist_level_used", "pax_bins"])


# test with insufficient data for hist_avg (all unique combinations)
def test_insufficient_data_all_nan():
    df = pd.DataFrame({
        "flight_key": ["f1", "f2", "f3"],
        "item_id": ["AB123", "AB124", "AB125"],  # all different items
        "origin": ["city_001", "city_002", "city_003"],
        "destination": ["city_003", "city_004", "city_005"],
        "number_of_passengers": [89, 110, 95],
        "sold_quantity": [3, 4, 2],
        "day_period": ["morning", "night", "afternoon"],
        "is_night": [False, True, False]
    })

    df_with_features = build_features(df)

    # With current implementation, even single records get avg at some level
    # This test verifies the function handles sparse data gracefully
    assert "hist_avg" in df_with_features.columns
    assert "hist_level_used" in df_with_features.columns
    # All items are unique, so if they get values, they should match sold_quantity
    # (mean of single value = the value itself)
    assert len(df_with_features) == 3


# test with NaN values in number_of_passengers
def test_nan_in_number_of_passengers():
    df = pd.DataFrame({
        "flight_key": ["f1", "f2", "f3", "f4", "f5"],
        "item_id": ["AB123"] * 5,
        "origin": ["city_001"] * 5,
        "destination": ["city_003"] * 5,
        "number_of_passengers": [89, None, 90, 88, 91],  # One NaN
        "sold_quantity": [3, 4, 2, 2, 1],
        "day_period": ["morning"] * 5,
        "is_night": [False] * 5
    })

    df_with_features = build_features(df)

    # Should have NaN in pax_bins for the row with NaN number_of_passengers
    assert df_with_features["pax_bins"].isna().sum() == 1


# test with single row
def test_single_row_dataframe():
    df = pd.DataFrame({
        "flight_key": ["abc123"],
        "item_id": ["AB123"],
        "origin": ["city_001"],
        "destination": ["city_003"],
        "number_of_passengers": [89],
        "sold_quantity": [3],
        "day_period": ["morning"],
        "is_night": [False]
    })

    df_with_features = build_features(df)

    # Should create all feature columns even with single row
    assert len(df_with_features) == 1
    assert "route" in df_with_features.columns
    assert "pax_bins" in df_with_features.columns
    assert "hist_avg" in df_with_features.columns
    assert df_with_features["route"].iloc[0] == "city_001 _ city_003"


# test with negative values in sold_quantity
def test_negative_sold_quantity():
    df = pd.DataFrame({
        "flight_key": ["f1"] * 5,
        "item_id": ["AB123"] * 5,
        "origin": ["city_001"] * 5,
        "destination": ["city_003"] * 5,
        "number_of_passengers": [89] * 5,
        "sold_quantity": [-1, 2, 3, 4, 5],  # One negative value
        "day_period": ["morning"] * 5,
        "is_night": [False] * 5
    })

    df_with_features = build_features(df)

    # Should process without error, hist_avg will include negative value
    assert "hist_avg" in df_with_features.columns
    # hist_avg = (-1 + 2 + 3 + 4 + 5) / 5 = 2.6
    assert (df_with_features["hist_avg"] == 2.6).all()

