import pytest
from pathlib import Path
from unittest.mock import patch, mock_open

from forecasting.utils.config_handler import return_config, FORECASTING_PATH


# Test FORECASTING_PATH
def test_forecasting_path_is_path():
    assert isinstance(FORECASTING_PATH, Path)


def test_forecasting_path_exists():
    assert FORECASTING_PATH.exists()


def test_forecasting_path_is_directory():
    assert FORECASTING_PATH.is_dir()


def test_forecasting_path_contains_configs():
    configs_dir = FORECASTING_PATH / "configs"
    assert configs_dir.exists()
    assert configs_dir.is_dir()


# Test return_config
def test_return_config_returns_dict():
    config = return_config()
    assert isinstance(config, dict)


def test_return_config_has_required_keys():
    config = return_config()

    # Check top-level keys
    assert "data_preparation" in config
    assert "model" in config


def test_return_config_data_preparation_structure():
    config = return_config()

    data_prep = config["data_preparation"]
    assert "data_ingestion" in data_prep
    assert "feature_engineering" in data_prep

    # Check data_ingestion
    assert "data_source" in data_prep["data_ingestion"]

    # Check feature_engineering
    assert "pax_bins" in data_prep["feature_engineering"]
    assert "min_samples" in data_prep["feature_engineering"]


def test_return_config_model_structure():
    config = return_config()

    model = config["model"]
    assert "model_features" in model
    assert "catboost" in model
    assert "test_split_by_weeks" in model
    assert "degradation_threshold" in model

    # Check model_features
    features = model["model_features"]
    assert "categorical" in features
    assert "numerical" in features
    assert "target" in features


def test_return_config_catboost_structure():
    config = return_config()

    catboost = config["model"]["catboost"]
    assert "low_missed_sales" in catboost
    assert "low_wastage" in catboost
    assert "classifier" in catboost
    assert "regressor" in catboost


def test_return_config_reads_correct_file():
    config = return_config()

    # Verify it's reading the actual config by checking known values
    assert config["data_preparation"]["data_ingestion"]["data_source"] in ["mock", "database"]
    assert isinstance(config["data_preparation"]["feature_engineering"]["pax_bins"], list)
    assert isinstance(config["model"]["test_split_by_weeks"], int)


def test_return_config_pax_bins_is_list():
    config = return_config()
    pax_bins = config["data_preparation"]["feature_engineering"]["pax_bins"]

    assert isinstance(pax_bins, list)
    assert len(pax_bins) > 0
    assert all(isinstance(x, int) for x in pax_bins)


def test_return_config_thresholds_are_float():
    config = return_config()

    assert isinstance(config["model"]["catboost"]["low_missed_sales"], (int, float))
    assert isinstance(config["model"]["catboost"]["low_wastage"], (int, float))


def test_return_config_features_are_lists():
    config = return_config()
    features = config["model"]["model_features"]

    assert isinstance(features["categorical"], list)
    assert isinstance(features["numerical"], list)
    assert isinstance(features["target"], str)


def test_return_config_cached_vs_fresh():
    # Call twice and ensure we get the same structure
    config1 = return_config()
    config2 = return_config()

    assert config1.keys() == config2.keys()
    assert config1["model"]["test_split_by_weeks"] == config2["model"]["test_split_by_weeks"]


@patch("builtins.open", new_callable=mock_open, read_data="""
data_preparation:
  data_ingestion:
    data_source: mock
  feature_engineering:
    pax_bins: [0, 100, 150, 180, 300]
    min_samples: 3
model:
  model_features:
    categorical: ["item_id"]
    numerical: ["hist_avg"]
    target: sold_quantity
  catboost:
    low_missed_sales: 0.4
    low_wastage: 0.7
    classifier:
      iterations: 100
    regressor:
      iterations: 100
  test_split_by_weeks: 2
  degradation_threshold: 0.1
""")
def test_return_config_with_mock_file(mock_file):
    config = return_config()

    assert config["data_preparation"]["data_ingestion"]["data_source"] == "mock"
    assert config["model"]["model_features"]["target"] == "sold_quantity"
    assert config["model"]["catboost"]["low_missed_sales"] == 0.4


def test_config_file_exists():
    config_path = FORECASTING_PATH / "configs" / "config.yaml"
    assert config_path.exists()
    assert config_path.is_file()


def test_config_file_is_valid_yaml():
    config_path = FORECASTING_PATH / "configs" / "config.yaml"

    # Should not raise any exceptions
    import yaml
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    assert config is not None
    assert isinstance(config, dict)
