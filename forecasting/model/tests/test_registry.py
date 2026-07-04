import pytest
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from catboost import CatBoostClassifier, CatBoostRegressor
import pandas as pd
import numpy as np

from forecasting.model.model_registry.handle_model import (
    _read_versions,
    _write_versions,
    save_model,
    load_model,
    REGISTRY_PATH,
    VERSIONS_FILE,
    MAX_VERSIONS
)


@pytest.fixture
def temp_registry():
    """Create temporary registry directory for testing."""
    temp_dir = tempfile.mkdtemp()
    temp_registry_path = Path(temp_dir) / "model_registry" / "catboost"
    temp_registry_path.mkdir(parents=True, exist_ok=True)

    yield temp_registry_path

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_classifier():
    """Create a mock classifier for testing."""
    classifier = CatBoostClassifier(iterations=2, depth=2, random_seed=42, verbose=0)
    # Train with minimal data
    X = pd.DataFrame({'feature': [1, 2, 3, 4, 5]})
    y = pd.Series([0, 1, 0, 1, 1])
    classifier.fit(X, y)
    return classifier


@pytest.fixture
def mock_regressor():
    """Create a mock regressor for testing."""
    regressor = CatBoostRegressor(iterations=2, depth=2, random_seed=42, verbose=0)
    # Train with minimal data
    X = pd.DataFrame({'feature': [1, 2, 3, 4, 5]})
    y = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    regressor.fit(X, y)
    return regressor


# Test _read_versions
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_read_versions_file_not_exists(mock_versions_file):
    mock_versions_file.exists.return_value = False

    result = _read_versions()

    assert result == {"latest": None, "versions": []}


@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_read_versions_file_exists(mock_versions_file, temp_registry):
    # Create a temporary versions file
    versions_data = {"latest": "v2", "versions": ["v1", "v2"]}
    versions_file_path = temp_registry / "versions.json"

    with open(versions_file_path, "w") as f:
        json.dump(versions_data, f)

    mock_versions_file.exists.return_value = True

    # Mock open to read from temp file
    with patch('builtins.open', open):
        mock_versions_file.__enter__ = lambda self: open(versions_file_path, 'r')
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            result = _read_versions()

    assert result == versions_data


@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_read_versions_empty_file(mock_versions_file, temp_registry):
    # Create empty versions file
    versions_data = {"latest": None, "versions": []}
    versions_file_path = temp_registry / "versions.json"

    with open(versions_file_path, "w") as f:
        json.dump(versions_data, f)

    mock_versions_file.exists.return_value = True

    with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
        result = _read_versions()

    assert result == versions_data
    assert result["latest"] is None
    assert len(result["versions"]) == 0


# Test _write_versions
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_write_versions_success(mock_versions_file, temp_registry):
    versions_data = {"latest": "v1", "versions": ["v1"]}
    versions_file_path = temp_registry / "versions.json"

    with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
        _write_versions(versions_data)

    # Verify file was written correctly
    with open(versions_file_path, "r") as f:
        written_data = json.load(f)

    assert written_data == versions_data


@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_write_versions_multiple_versions(mock_versions_file, temp_registry):
    versions_data = {"latest": "v3", "versions": ["v1", "v2", "v3"]}
    versions_file_path = temp_registry / "versions.json"

    with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
        _write_versions(versions_data)

    with open(versions_file_path, "r") as f:
        written_data = json.load(f)

    assert written_data["latest"] == "v3"
    assert len(written_data["versions"]) == 3


# Test save_model
@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_save_model_first_version(mock_versions_file, mock_registry_path,
                                   temp_registry, mock_classifier, mock_regressor):
    mock_registry_path.__truediv__ = lambda self, other: temp_registry / other
    mock_versions_file.__truediv__ = lambda self, other: temp_registry / other
    versions_file_path = temp_registry / "versions.json"

    # No existing versions
    mock_versions_file.exists.return_value = False

    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            version = save_model(mock_classifier, mock_regressor)

    assert version == "v1"

    # Check that models were saved
    version_path = temp_registry / "v1"
    assert version_path.exists()
    assert (version_path / "classifier.cbm").exists()
    assert (version_path / "regressor.cbm").exists()

    # Check versions file
    with open(versions_file_path, "r") as f:
        versions_data = json.load(f)
    assert versions_data["latest"] == "v1"
    assert versions_data["versions"] == ["v1"]


@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_save_model_incremental_version(mock_versions_file, mock_registry_path,
                                        temp_registry, mock_classifier, mock_regressor):
    versions_file_path = temp_registry / "versions.json"

    # Create existing version
    existing_versions = {"latest": "v1", "versions": ["v1"]}
    with open(versions_file_path, "w") as f:
        json.dump(existing_versions, f)

    mock_versions_file.exists.return_value = True

    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            version = save_model(mock_classifier, mock_regressor)

    assert version == "v2"

    # Check versions file
    with open(versions_file_path, "r") as f:
        versions_data = json.load(f)
    assert versions_data["latest"] == "v2"
    assert versions_data["versions"] == ["v1", "v2"]


@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
@patch('forecasting.model.model_registry.handle_model.MAX_VERSIONS', 2)
def test_save_model_max_versions_cleanup(mock_versions_file, mock_registry_path,
                                         temp_registry, mock_classifier, mock_regressor):
    versions_file_path = temp_registry / "versions.json"

    # Create v1 and v2 (at max)
    v1_path = temp_registry / "v1"
    v1_path.mkdir(parents=True, exist_ok=True)
    (v1_path / "classifier.cbm").touch()
    (v1_path / "regressor.cbm").touch()

    v2_path = temp_registry / "v2"
    v2_path.mkdir(parents=True, exist_ok=True)
    (v2_path / "classifier.cbm").touch()
    (v2_path / "regressor.cbm").touch()

    existing_versions = {"latest": "v2", "versions": ["v1", "v2"]}
    with open(versions_file_path, "w") as f:
        json.dump(existing_versions, f)

    mock_versions_file.exists.return_value = True

    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            version = save_model(mock_classifier, mock_regressor)

    assert version == "v3"

    # v1 should be deleted, v2 and v3 should remain
    assert not (temp_registry / "v1").exists()
    assert (temp_registry / "v2").exists()
    assert (temp_registry / "v3").exists()

    # Check versions file
    with open(versions_file_path, "r") as f:
        versions_data = json.load(f)
    assert versions_data["latest"] == "v3"
    assert versions_data["versions"] == ["v2", "v3"]


# Test load_model
@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_load_model_success(mock_versions_file, mock_registry_path,
                            temp_registry, mock_classifier, mock_regressor):
    versions_file_path = temp_registry / "versions.json"
    version_path = temp_registry / "v1"
    version_path.mkdir(parents=True, exist_ok=True)

    # Save models
    mock_classifier.save_model(str(version_path / "classifier.cbm"))
    mock_regressor.save_model(str(version_path / "regressor.cbm"))

    # Write versions file
    versions_data = {"latest": "v1", "versions": ["v1"]}
    with open(versions_file_path, "w") as f:
        json.dump(versions_data, f)

    mock_versions_file.exists.return_value = True

    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            classifier, regressor = load_model()

    # Check that models were loaded
    assert isinstance(classifier, CatBoostClassifier)
    assert isinstance(regressor, CatBoostRegressor)


@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_load_model_no_models(mock_versions_file, mock_registry_path, temp_registry):
    versions_file_path = temp_registry / "versions.json"

    # No models in registry
    versions_data = {"latest": None, "versions": []}
    with open(versions_file_path, "w") as f:
        json.dump(versions_data, f)

    mock_versions_file.exists.return_value = True

    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            with pytest.raises(FileNotFoundError, match="No models found in registry"):
                load_model()


@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_load_model_loads_latest(mock_versions_file, mock_registry_path,
                                 temp_registry, mock_classifier, mock_regressor):
    versions_file_path = temp_registry / "versions.json"

    # Create v1 and v2
    v1_path = temp_registry / "v1"
    v1_path.mkdir(parents=True, exist_ok=True)
    mock_classifier.save_model(str(v1_path / "classifier.cbm"))
    mock_regressor.save_model(str(v1_path / "regressor.cbm"))

    v2_path = temp_registry / "v2"
    v2_path.mkdir(parents=True, exist_ok=True)
    mock_classifier.save_model(str(v2_path / "classifier.cbm"))
    mock_regressor.save_model(str(v2_path / "regressor.cbm"))

    # Set v2 as latest
    versions_data = {"latest": "v2", "versions": ["v1", "v2"]}
    with open(versions_file_path, "w") as f:
        json.dump(versions_data, f)

    mock_versions_file.exists.return_value = True

    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            classifier, regressor = load_model()

    # Models should be loaded from v2
    assert isinstance(classifier, CatBoostClassifier)
    assert isinstance(regressor, CatBoostRegressor)


# Integration test: save and load
@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_save_and_load_integration(mock_versions_file, mock_registry_path,
                                   temp_registry, mock_classifier, mock_regressor):
    versions_file_path = temp_registry / "versions.json"
    mock_versions_file.exists.return_value = False

    # Save model
    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            version = save_model(mock_classifier, mock_regressor)

    assert version == "v1"

    # Load model
    mock_versions_file.exists.return_value = True
    with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
        with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
            loaded_classifier, loaded_regressor = load_model()

    # Verify loaded models work
    assert isinstance(loaded_classifier, CatBoostClassifier)
    assert isinstance(loaded_regressor, CatBoostRegressor)

    # Test predictions
    X_test = pd.DataFrame({'feature': [1, 2, 3]})
    cls_pred = loaded_classifier.predict(X_test)
    reg_pred = loaded_regressor.predict(X_test)

    assert len(cls_pred) == 3
    assert len(reg_pred) == 3


@patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH')
@patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE')
def test_save_model_multiple_times(mock_versions_file, mock_registry_path,
                                   temp_registry, mock_classifier, mock_regressor):
    versions_file_path = temp_registry / "versions.json"
    mock_versions_file.exists.return_value = False

    # Save model 3 times
    versions = []
    for i in range(3):
        mock_versions_file.exists.return_value = (i > 0)
        with patch('forecasting.model.model_registry.handle_model.REGISTRY_PATH', temp_registry):
            with patch('forecasting.model.model_registry.handle_model.VERSIONS_FILE', versions_file_path):
                version = save_model(mock_classifier, mock_regressor)
                versions.append(version)

    assert versions == ["v1", "v2", "v3"]

    # Check final state
    with open(versions_file_path, "r") as f:
        versions_data = json.load(f)

    assert versions_data["latest"] == "v3"
    # Due to MAX_VERSIONS=2, only v2 and v3 should remain
    assert versions_data["versions"] == ["v2", "v3"]
