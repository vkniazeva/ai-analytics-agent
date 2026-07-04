import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from forecasting.api.app import (
    app,
    _map_bins,
    _load_fresh_products,
    _load_hist_avg,
    _prepare_data,
    _process_classification,
    _process_regression,
    _get_estimated_accuracy
)


@pytest.fixture
def client():
    """Create test client for FastAPI app."""
    return TestClient(app)


@pytest.fixture
def mock_models():
    """Create mock classifier and regressor."""
    classifier = MagicMock()
    classifier.predict_proba.return_value = np.array([[0.3, 0.7], [0.6, 0.4], [0.2, 0.8]])

    regressor = MagicMock()
    regressor.predict.return_value = np.array([5.0, 3.0, 7.0])

    return classifier, regressor


# Test _map_bins
def test_map_bins_less_than_100():
    result = _map_bins(50)
    assert result == "<100"


def test_map_bins_100_to_150():
    result = _map_bins(125)
    assert result == "100 - 150"


def test_map_bins_150_to_180():
    result = _map_bins(165)
    assert result == "150 - 180"


def test_map_bins_more_than_180():
    result = _map_bins(200)
    assert result == "180 +"


def test_map_bins_boundary_values():
    assert _map_bins(99) == "<100"
    assert _map_bins(100) == "100 - 150"
    assert _map_bins(150) == "150 - 180"
    assert _map_bins(180) == "180 +"


def test_map_bins_edge_cases():
    assert _map_bins(0) == "<100"
    assert _map_bins(1000) == "180 +"


# Test _load_fresh_products
@patch('forecasting.api.app.read_sql')
def test_load_fresh_products_mock_mode(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'C3L2W001'],
        'category': ['Beverages', 'Snacks']
    })

    with patch('forecasting.api.app.data_source', 'mock'):
        result = _load_fresh_products()

    assert len(result) == 2
    assert 'item_id' in result.columns
    assert 'category' in result.columns
    mock_read_sql.assert_called_once_with(
        "SELECT item_id, category FROM mart.dim_products WHERE item_type = 'Fresh Product' AND category != 'BOL Products' AND status = 'Active'",
        "dim_products"
    )


@patch('forecasting.api.app.read_sql')
def test_load_fresh_products_database_mode(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'category': ['Beverages']
    })

    with patch('forecasting.api.app.data_source', 'database'):
        result = _load_fresh_products()

    mock_read_sql.assert_called_once()
    # In database mode, should be called without table_name
    assert mock_read_sql.call_args[0][0].startswith("SELECT item_id, category")


# Test _load_hist_avg
@patch('forecasting.api.app.read_sql')
def test_load_hist_avg(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'route': ['city_001 _ city_029'],
        'day_period': ['Morning'],
        'hist_avg': [3.5]
    })

    with patch('forecasting.api.app.data_source', 'mock'):
        result = _load_hist_avg()

    assert len(result) == 1
    assert 'hist_avg' in result.columns


# Test _prepare_data
@patch('forecasting.api.app._load_hist_avg')
@patch('forecasting.api.app._load_fresh_products')
def test_prepare_data_success(mock_load_products, mock_load_hist_avg):
    mock_load_products.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002', 'C3L2W001']
    })

    mock_load_hist_avg.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002'],
        'route': ['city_001 _ city_029', 'city_001 _ city_029'],
        'day_period': ['Morning', 'Morning'],
        'hist_avg': [3.5, 2.0]
    })

    result = _prepare_data(
        route='city_001 _ city_029',
        day_period='Morning',
        expected_pax=89
    )

    assert len(result) == 3
    assert 'item_id' in result.columns
    assert 'route' in result.columns
    assert 'pax_bin' in result.columns
    assert 'day_period' in result.columns
    assert 'hist_avg' in result.columns
    assert (result['pax_bin'] == '<100').all()
    assert (result['route'] == 'city_001 _ city_029').all()


@patch('forecasting.api.app._load_hist_avg')
@patch('forecasting.api.app._load_fresh_products')
def test_prepare_data_with_different_pax_bins(mock_load_products, mock_load_hist_avg):
    mock_load_products.return_value = pd.DataFrame({'item_id': ['T3L4D001']})
    mock_load_hist_avg.return_value = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'route': ['city_001 _ city_029'],
        'day_period': ['Morning'],
        'hist_avg': [3.5]
    })

    # Test different pax bins
    result_low = _prepare_data('city_001 _ city_029', 'Morning', 50)
    assert (result_low['pax_bin'] == '<100').all()

    result_mid = _prepare_data('city_001 _ city_029', 'Morning', 125)
    assert (result_mid['pax_bin'] == '100 - 150').all()

    result_high = _prepare_data('city_001 _ city_029', 'Morning', 200)
    assert (result_high['pax_bin'] == '180 +').all()


# Test _process_classification
def test_process_classification_success():
    df = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002', 'T3L4D003'],
        'route': ['city_001 _ city_029'] * 3,
        'pax_bin': ['<100'] * 3,
        'day_period': ['Morning'] * 3,
        'hist_avg': [3.5, 2.0, 1.5]
    })

    classifier = MagicMock()
    classifier.predict_proba.return_value = np.array([
        [0.3, 0.7],  # Prob > 0.5, predict 1
        [0.6, 0.4],  # Prob < 0.5, predict 0
        [0.2, 0.8]   # Prob > 0.5, predict 1
    ])

    features = ['item_id', 'route', 'pax_bin', 'day_period', 'hist_avg']
    threshold_type = 'low_missed_sales'

    with patch('forecasting.api.app.config', {
        'model': {'catboost': {'low_missed_sales': 0.5}}
    }):
        result = _process_classification(df, classifier, threshold_type, features)

    assert 'cls_predict_proba' in result.columns
    assert 'cls_predict' in result.columns
    assert result['cls_predict'].tolist() == [1, 0, 1]


def test_process_classification_different_thresholds():
    df = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'route': ['city_001 _ city_029'],
        'pax_bin': ['<100'],
        'day_period': ['Morning'],
        'hist_avg': [3.5]
    })

    classifier = MagicMock()
    classifier.predict_proba.return_value = np.array([[0.4, 0.6]])

    features = ['item_id', 'route', 'pax_bin', 'day_period', 'hist_avg']

    # With threshold 0.5, should predict 1
    with patch('forecasting.api.app.config', {
        'model': {'catboost': {'low_missed_sales': 0.5}}
    }):
        result1 = _process_classification(df, classifier, 'low_missed_sales', features)
        assert result1['cls_predict'].iloc[0] == 1

    # With threshold 0.7, should predict 0
    with patch('forecasting.api.app.config', {
        'model': {'catboost': {'low_wastage': 0.7}}
    }):
        result2 = _process_classification(df, classifier, 'low_wastage', features)
        assert result2['cls_predict'].iloc[0] == 0


# Test _process_regression
def test_process_regression_success():
    df = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002', 'T3L4D003'],
        'cls_predict': [1, 0, 1],
        'route': ['city_001 _ city_029'] * 3,
        'pax_bin': ['<100'] * 3,
        'day_period': ['Morning'] * 3,
        'hist_avg': [3.5, 2.0, 1.5]
    })

    regressor = MagicMock()
    regressor.predict.return_value = np.array([5.3, 7.8])  # Only for cls_predict=1

    features = ['item_id', 'route', 'pax_bin', 'day_period', 'hist_avg']

    result = _process_regression(df, regressor, features)

    assert 'predicted' in result.columns
    assert len(result) == 3
    # cls_predict=0 should have predicted=0
    assert result[result['item_id'] == 'T3L4D002']['predicted'].iloc[0] == 0
    # cls_predict=1 should have predicted values (rounded)
    assert result[result['item_id'] == 'T3L4D001']['predicted'].iloc[0] == 5
    assert result[result['item_id'] == 'T3L4D003']['predicted'].iloc[0] == 8


def test_process_regression_all_zeros():
    df = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002'],
        'cls_predict': [0, 0],
        'route': ['city_001 _ city_029'] * 2,
        'pax_bin': ['<100'] * 2,
        'day_period': ['Morning'] * 2,
        'hist_avg': [3.5, 2.0]
    })

    regressor = MagicMock()
    features = ['item_id', 'route', 'pax_bin', 'day_period', 'hist_avg']

    result = _process_regression(df, regressor, features)

    # All should be 0
    assert (result['predicted'] == 0).all()
    # Regressor should not be called
    regressor.predict.assert_not_called()


def test_process_regression_negative_predictions():
    df = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'cls_predict': [1],
        'route': ['city_001 _ city_029'],
        'pax_bin': ['<100'],
        'day_period': ['Morning'],
        'hist_avg': [3.5]
    })

    regressor = MagicMock()
    regressor.predict.return_value = np.array([-2.5])  # Negative value

    features = ['item_id', 'route', 'pax_bin', 'day_period', 'hist_avg']

    result = _process_regression(df, regressor, features)

    # With .clip(0), negative values should be clipped to 0
    assert result['predicted'].iloc[0] == 0


# Test _get_estimated_accuracy
@patch('forecasting.api.app.read_sql')
def test_get_estimated_accuracy_found(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame({
        'estimated_accuracy': [0.85]
    })

    result = _get_estimated_accuracy('T3L4D001')

    assert result == 0.85


@patch('forecasting.api.app.read_sql')
def test_get_estimated_accuracy_not_found(mock_read_sql):
    mock_read_sql.return_value = pd.DataFrame()

    result = _get_estimated_accuracy('T3L4D001')

    assert result is None


# Test API endpoints
@patch('forecasting.api.app._load_hist_avg')
@patch('forecasting.api.app._load_fresh_products')
def test_predict_all_items_endpoint(mock_load_products, mock_load_hist_avg, client):
    mock_load_products.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002']
    })

    mock_load_hist_avg.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002'],
        'route': ['city_001 _ city_029'] * 2,
        'day_period': ['Morning'] * 2,
        'hist_avg': [3.5, 2.0]
    })

    # Mock models in app state
    mock_classifier = MagicMock()
    # Threshold for low_missed_sales = 0.4
    # Item 1: proba=0.7 >= 0.4 -> cls_predict=1
    # Item 2: proba=0.3 < 0.4 -> cls_predict=0
    mock_classifier.predict_proba.return_value = np.array([[0.7, 0.3], [0.7, 0.3]])

    mock_regressor = MagicMock()
    # Only 1 item has cls_predict=1, so regressor returns array of length 1
    mock_regressor.predict.return_value = np.array([5.0])

    app.state.classifier = mock_classifier
    app.state.regressor = mock_regressor

    response = client.post(
        "/predict/low_missed_sales",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89,
            "day_period": "Morning"
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2
    assert all('item_id' in item for item in data)
    assert all('predicted_quantity' in item for item in data)


@patch('forecasting.api.app._load_hist_avg')
@patch('forecasting.api.app._load_fresh_products')
@patch('forecasting.api.app._get_estimated_accuracy')
def test_predict_item_endpoint(mock_get_accuracy, mock_load_products, mock_load_hist_avg, client):
    mock_load_products.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002']
    })

    mock_load_hist_avg.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002'],
        'route': ['city_001 _ city_029'] * 2,
        'day_period': ['Morning'] * 2,
        'hist_avg': [3.5, 2.0]
    })

    mock_get_accuracy.return_value = 0.85

    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.array([[0.3, 0.7]])

    mock_regressor = MagicMock()
    mock_regressor.predict.return_value = np.array([5.0])

    app.state.classifier = mock_classifier
    app.state.regressor = mock_regressor

    response = client.post(
        "/predict/low_missed_sales/item/T3L4D001",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89,
            "day_period": "Morning"
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert data['item_id'] == 'T3L4D001'
    assert 'predicted_quantity' in data
    assert 'threshold_type' in data
    assert 'threshold_value' in data
    assert 'historical_average' in data
    assert 'estimated_accuracy' in data


@patch('forecasting.api.app._load_hist_avg')
@patch('forecasting.api.app._load_fresh_products')
def test_predict_item_not_found(mock_load_products, mock_load_hist_avg, client):
    mock_load_products.return_value = pd.DataFrame({
        'item_id': ['T3L4D001']
    })

    mock_load_hist_avg.return_value = pd.DataFrame({
        'item_id': ['T3L4D001'],
        'route': ['city_001 _ city_029'],
        'day_period': ['Morning'],
        'hist_avg': [3.5]
    })

    response = client.post(
        "/predict/low_missed_sales/item/NONEXISTENT",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89,
            "day_period": "Morning"
        }
    )

    assert response.status_code == 404
    assert "not found" in response.json()['detail'].lower()


@patch('forecasting.api.app._load_hist_avg')
@patch('forecasting.api.app._load_fresh_products')
def test_predict_by_category_endpoint(mock_load_products, mock_load_hist_avg, client):
    mock_load_products.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002', 'C3L2W001'],
        'category': ['Beverages', 'Beverages', 'Snacks']
    })

    mock_load_hist_avg.return_value = pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002', 'C3L2W001'],
        'route': ['city_001 _ city_029'] * 3,
        'day_period': ['Morning'] * 3,
        'hist_avg': [3.5, 2.0, 1.5]
    })

    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.array([[0.3, 0.7], [0.6, 0.4], [0.2, 0.8]])

    mock_regressor = MagicMock()
    mock_regressor.predict.return_value = np.array([5.0, 7.0])

    app.state.classifier = mock_classifier
    app.state.regressor = mock_regressor

    response = client.post(
        "/predict/low_wastage/category",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89,
            "day_period": "Morning"
        }
    )

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 2  # Beverages and Snacks
    assert all('category_name' in item for item in data)
    assert all('predicted_quantity' in item for item in data)


def test_api_invalid_threshold_type(client):
    response = client.post(
        "/predict/invalid_threshold",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89,
            "day_period": "Morning"
        }
    )

    assert response.status_code == 422  # Validation error


def test_api_invalid_day_period(client):
    response = client.post(
        "/predict/low_missed_sales",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89,
            "day_period": "InvalidPeriod"
        }
    )

    assert response.status_code == 422  # Validation error


def test_api_missing_required_field(client):
    response = client.post(
        "/predict/low_missed_sales",
        json={
            "route": "city_001 _ city_029",
            "expected_pax": 89
            # missing day_period
        }
    )

    assert response.status_code == 422  # Validation error
