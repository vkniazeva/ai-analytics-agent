import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch

from forecasting.data_preparation.feature_engineering.feature_engineering import (
    build_features,
    _create_route,
    _create_pax_bins,
    _create_hist_avg
)


# Helper function to create valid dataframe
def create_valid_dataframe():
    return pd.DataFrame({
        "flight_key": ["f1", "f2", "f3", "f4", "f5", "f6", "f7"],
        "item_id": ["AB123", "AB123", "AB124", "AB124", "AB124", "AB124", "AB124"],
        "origin": ["city_001", "city_002", "city_001", "city_001", "city_001", "city_001", "city_001"],
        "destination": ["city_003", "city_004", "city_003", "city_003", "city_003", "city_003", "city_003"],
        "number_of_passengers": [89, 110, 89, 89, 89, 89, 89],
        "sold_quantity": [3, 4, 2, 2, 1, 4, 3],
        "day_period": ["morning", "night", "morning", "morning", "morning", "morning", "morning"],
        "is_night": [False, True, False, False, False, False, False]
    })


# Test _create_route
def test_create_route_success():
    df = pd.DataFrame({
        "origin": ["city_001", "city_002", "city_003"],
        "destination": ["city_003", "city_004", "city_005"]
    })

    result = _create_route(df)

    assert "route" in result.columns
    assert result["route"].iloc[0] == "city_001 _ city_003"
    assert result["route"].iloc[1] == "city_002 _ city_004"
    assert result["route"].iloc[2] == "city_003 _ city_005"


def test_create_route_preserves_original_columns():
    df = pd.DataFrame({
        "origin": ["city_001"],
        "destination": ["city_003"],
        "other_col": ["value"]
    })

    result = _create_route(df)

    assert "origin" in result.columns
    assert "destination" in result.columns
    assert "other_col" in result.columns


def test_create_route_with_special_characters():
    df = pd.DataFrame({
        "origin": ["city-001", "city/002"],
        "destination": ["city_003", "city 004"]
    })

    result = _create_route(df)

    assert result["route"].iloc[0] == "city-001 _ city_003"
    assert result["route"].iloc[1] == "city/002 _ city 004"


# Test _create_pax_bins
def test_create_pax_bins_success():
    df = pd.DataFrame({
        "number_of_passengers": [50, 125, 165, 185, 250]
    })
    pax_bins_config = [0, 100, 150, 180, 300]

    result = _create_pax_bins(df, pax_bins_config)

    assert "pax_bin" in result.columns
    assert result["pax_bin"].iloc[0] == "<100"
    assert result["pax_bin"].iloc[1] == "100 - 150"
    assert result["pax_bin"].iloc[2] == "150 - 180"
    assert result["pax_bin"].iloc[3] == "180 +"
    assert result["pax_bin"].iloc[4] == "180 +"


def test_create_pax_bins_boundary_values():
    df = pd.DataFrame({
        "number_of_passengers": [0, 100, 150, 180]
    })
    pax_bins_config = [0, 100, 150, 180, 300]

    result = _create_pax_bins(df, pax_bins_config)

    # pd.cut with include_lowest=True creates: [0,100], (100,150], (150,180], (180,300]
    # So boundary values on the right edge belong to their interval
    assert result["pax_bin"].iloc[0] == "<100"  # 0 in [0, 100]
    assert result["pax_bin"].iloc[1] == "<100"  # 100 in [0, 100]
    assert result["pax_bin"].iloc[2] == "100 - 150"  # 150 in (100, 150]
    assert result["pax_bin"].iloc[3] == "150 - 180"  # 180 in (150, 180]


def test_create_pax_bins_with_nan():
    df = pd.DataFrame({
        "number_of_passengers": [89, None, 110, np.nan]
    })
    pax_bins_config = [0, 100, 150, 180, 300]

    result = _create_pax_bins(df, pax_bins_config)

    # NaN values should result in NaN bins
    assert result["pax_bin"].notna().sum() == 2
    assert result["pax_bin"].iloc[0] == "<100"
    assert pd.isna(result["pax_bin"].iloc[1])
    assert result["pax_bin"].iloc[2] == "100 - 150"


# Test _create_hist_avg
def test_create_hist_avg_level1_sufficient_data():
    # Level 1: item_id + route + day_period
    df = pd.DataFrame({
        "item_id": ["AB123"] * 5,
        "origin": ["city_001"] * 5,
        "destination": ["city_003"] * 5,
        "day_period": ["morning"] * 5,
        "is_night": [False] * 5,
        "sold_quantity": [1, 2, 3, 4, 5]
    })
    df = _create_route(df)
    min_samples = 3

    result = _create_hist_avg(df, min_samples)

    # Should use level 1 (has 5 samples >= min_samples=3)
    assert "hist_avg" in result.columns
    assert "hist_level_used" in result.columns
    assert (result["hist_level_used"] == 1).all()
    assert (result["hist_avg"] == 3.0).all()  # mean of [1,2,3,4,5]


def test_create_hist_avg_fallback_to_level4():
    # Different combinations, should fall back to level 4 (item_id only)
    df = pd.DataFrame({
        "item_id": ["AB123", "AB123", "AB123"],
        "origin": ["city_001", "city_002", "city_003"],
        "destination": ["city_003", "city_004", "city_005"],
        "day_period": ["morning", "night", "afternoon"],
        "is_night": [False, True, False],
        "sold_quantity": [10, 20, 30]
    })
    df = _create_route(df)
    min_samples = 5  # High threshold, will force level 4

    result = _create_hist_avg(df, min_samples)

    # Should use level 4 (all rows have same item_id)
    assert (result["hist_level_used"] == 4).all()
    assert (result["hist_avg"] == 20.0).all()  # mean of [10,20,30]


def test_create_hist_avg_multiple_items():
    df = pd.DataFrame({
        "item_id": ["AB123"] * 3 + ["AB124"] * 3,
        "origin": ["city_001"] * 6,
        "destination": ["city_003"] * 6,
        "day_period": ["morning"] * 6,
        "is_night": [False] * 6,
        "sold_quantity": [1, 2, 3, 10, 20, 30]
    })
    df = _create_route(df)
    min_samples = 3

    result = _create_hist_avg(df, min_samples)

    # Both items have 3 samples at level 1
    assert (result["hist_level_used"] == 1).all()
    assert (result[result["item_id"] == "AB123"]["hist_avg"] == 2.0).all()
    assert (result[result["item_id"] == "AB124"]["hist_avg"] == 20.0).all()


def test_create_hist_avg_mixed_levels():
    # AB123 has enough data for level 1, AB124 falls back to level 4
    df = pd.DataFrame({
        "item_id": ["AB123"] * 5 + ["AB124"] * 2,
        "origin": ["city_001"] * 5 + ["city_002", "city_003"],
        "destination": ["city_003"] * 5 + ["city_004", "city_005"],
        "day_period": ["morning"] * 5 + ["night", "afternoon"],
        "is_night": [False] * 5 + [True, False],
        "sold_quantity": [1, 2, 3, 4, 5, 10, 20]
    })
    df = _create_route(df)
    min_samples = 5

    result = _create_hist_avg(df, min_samples)

    # AB123 uses level 1 (5 samples)
    assert (result[result["item_id"] == "AB123"]["hist_level_used"] == 1).all()
    assert (result[result["item_id"] == "AB123"]["hist_avg"] == 3.0).all()

    # AB124 uses level 4 (only 2 samples total)
    assert (result[result["item_id"] == "AB124"]["hist_level_used"] == 4).all()
    assert (result[result["item_id"] == "AB124"]["hist_avg"] == 15.0).all()


def test_create_hist_avg_no_temporary_columns():
    df = create_valid_dataframe()
    df = _create_route(df)

    result = _create_hist_avg(df, min_samples=1)

    # Temporary columns should be removed
    temp_columns = [c for c in result.columns if c.startswith("hist_avg_l") or c.startswith("hist_count_l")]
    assert len(temp_columns) == 0


# Test build_features (integration)
@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_success(mock_write_sql):
    df = create_valid_dataframe()

    result = build_features(df)

    # All feature columns should be present
    assert "route" in result.columns
    assert "pax_bin" in result.columns
    assert "hist_avg" in result.columns
    assert "hist_level_used" in result.columns

    # Should have 4 new columns
    assert len(result.columns) == len(df.columns) + 4

    # All rows should remain
    assert len(result) == len(df)


@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_verify_all_features(mock_write_sql):
    df = create_valid_dataframe()

    result = build_features(df)

    # Verify route creation
    assert (result[result["origin"] == "city_001"]["route"].str.contains("city_001 _ ")).all()

    # Verify pax_bin
    assert result["pax_bin"].notna().sum() > 0
    assert "<100" in result["pax_bin"].values
    assert "100 - 150" in result["pax_bin"].values

    # Verify hist_avg
    assert result["hist_avg"].notna().all()
    assert result["hist_level_used"].notna().all()


@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_does_not_modify_original(mock_write_sql):
    df = create_valid_dataframe()
    original_columns = df.columns.tolist()

    build_features(df)

    # Original dataframe should not be modified
    assert df.columns.tolist() == original_columns
    assert "route" not in df.columns


# Test missing columns
def test_build_features_missing_origin():
    df = pd.DataFrame({
        "flight_key": ["f1"],
        "item_id": ["AB123"],
        "destination": ["city_003"],
        "number_of_passengers": [89],
        "sold_quantity": [3],
        "day_period": ["morning"],
        "is_night": [False]
    })

    with pytest.raises(KeyError):
        build_features(df)


def test_build_features_missing_multiple_columns():
    df = pd.DataFrame({
        "flight_key": ["f1"],
        "item_id": ["AB123"]
    })

    with pytest.raises(KeyError):
        build_features(df)


# Edge cases
@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_empty_dataframe(mock_write_sql):
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

    result = build_features(df)

    # Should return empty dataframe with all feature columns
    assert len(result) == 0
    assert all(col in result.columns for col in ["route", "hist_avg", "hist_level_used", "pax_bin"])


@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_single_row(mock_write_sql):
    df = pd.DataFrame({
        "flight_key": ["f1"],
        "item_id": ["AB123"],
        "origin": ["city_001"],
        "destination": ["city_003"],
        "number_of_passengers": [89],
        "sold_quantity": [3],
        "day_period": ["morning"],
        "is_night": [False]
    })

    result = build_features(df)

    # Should create all features even with single row
    assert len(result) == 1
    assert result["route"].iloc[0] == "city_001 _ city_003"
    assert result["pax_bin"].iloc[0] == "<100"
    assert result["hist_avg"].iloc[0] == 3.0  # Mean of single value
    assert result["hist_level_used"].iloc[0] == 4.0  # Falls back to level 4


@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_all_unique_items(mock_write_sql):
    df = pd.DataFrame({
        "flight_key": ["f1", "f2", "f3"],
        "item_id": ["AB123", "AB124", "AB125"],
        "origin": ["city_001", "city_002", "city_003"],
        "destination": ["city_003", "city_004", "city_005"],
        "number_of_passengers": [89, 110, 95],
        "sold_quantity": [3, 4, 2],
        "day_period": ["morning", "night", "afternoon"],
        "is_night": [False, True, False]
    })

    result = build_features(df)

    # Should process successfully
    assert len(result) == 3
    assert "hist_avg" in result.columns
    # Each item has its own sold_quantity as hist_avg (mean of single value)
    assert result["hist_avg"].tolist() == [3.0, 4.0, 2.0]


@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_with_nan_passengers(mock_write_sql):
    df = pd.DataFrame({
        "flight_key": ["f1", "f2", "f3"],
        "item_id": ["AB123"] * 3,
        "origin": ["city_001"] * 3,
        "destination": ["city_003"] * 3,
        "number_of_passengers": [89, None, 90],
        "sold_quantity": [3, 4, 2],
        "day_period": ["morning"] * 3,
        "is_night": [False] * 3
    })

    result = build_features(df)

    # Should have NaN in pax_bin for row with NaN passengers
    assert result["pax_bin"].isna().sum() == 1
    # But hist_avg should be calculated for all rows
    assert result["hist_avg"].notna().all()


@patch('forecasting.data_preparation.feature_engineering.feature_engineering.write_sql')
def test_build_features_negative_sold_quantity(mock_write_sql):
    df = pd.DataFrame({
        "flight_key": ["f1"] * 5,
        "item_id": ["AB123"] * 5,
        "origin": ["city_001"] * 5,
        "destination": ["city_003"] * 5,
        "number_of_passengers": [89] * 5,
        "sold_quantity": [-1, 2, 3, 4, 5],
        "day_period": ["morning"] * 5,
        "is_night": [False] * 5
    })

    result = build_features(df)

    # Should process without error
    assert "hist_avg" in result.columns
    # hist_avg includes negative value: (-1 + 2 + 3 + 4 + 5) / 5 = 2.6
    expected_avg = (-1 + 2 + 3 + 4 + 5) / 5
    assert (result["hist_avg"] == expected_avg).all()
