import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from forecasting.model.model_evaluation.evaluate import (
    _create_test_set,
    _evaluate_classifier,
    _evaluate_regressor,
    _evaluate_business_metrics,
    _evaluate_business_metrics_by_item,
    _check_degradation
)


# Helper function to create valid dataframe
def create_valid_dataframe():
    dates = pd.date_range(start='2025-01-01', end='2025-12-31', freq='D')
    n = len(dates)
    np.random.seed(42)

    return pd.DataFrame({
        'date': dates,
        'item_id': (['T3L4D001', 'T3L4D002', 'C3L2W001'] * (n // 3 + 1))[:n],
        'route': (['city_001 _ city_003', 'city_002 _ city_004'] * (n // 2 + 1))[:n],
        'day_period': (['Morning', 'Day', 'Evening'] * (n // 3 + 1))[:n],
        'pax_bin': (['<100', '100 - 150'] * (n // 2 + 1))[:n],
        'hist_avg': np.random.uniform(0, 3, n),
        'sold_quantity': np.random.randint(0, 5, n)
    })


# Test _create_test_set
def test_create_test_set_success():
    df = create_valid_dataframe()
    weeks_split = 2
    features = ['item_id', 'route', 'day_period', 'pax_bin', 'hist_avg']
    target = 'sold_quantity'

    X_test, y_test_cls, y_test, item_ids = _create_test_set(df, weeks_split, features, target)

    # Check return types
    assert isinstance(X_test, pd.DataFrame)
    assert isinstance(y_test_cls, pd.Series)
    assert isinstance(y_test, pd.Series)
    assert isinstance(item_ids, pd.Series)

    # Check that all have same length
    assert len(X_test) == len(y_test_cls) == len(y_test) == len(item_ids)

    # Check that test set is not empty
    assert len(X_test) > 0

    # Check features are correct
    assert list(X_test.columns) == features

    # Check that y_test_cls is binary
    assert set(y_test_cls.unique()).issubset({0, 1})


def test_create_test_set_correct_split():
    df = create_valid_dataframe()
    weeks_split = 2
    features = ['item_id', 'route']
    target = 'sold_quantity'

    X_test, y_test_cls, y_test, item_ids = _create_test_set(df, weeks_split, features, target)

    # Check that test set contains only recent weeks
    cutoff_date = df["date"].max() - pd.Timedelta(weeks=weeks_split)
    test_dates = df[df.index.isin(X_test.index)]["date"]
    assert all(test_dates > cutoff_date)


def test_create_test_set_binary_classification():
    df = create_valid_dataframe()
    weeks_split = 2
    features = ['item_id']
    target = 'sold_quantity'

    X_test, y_test_cls, y_test, item_ids = _create_test_set(df, weeks_split, features, target)

    # y_test_cls should be 1 where sold_quantity > 0, and 0 otherwise
    test_df = df[df["date"] > (df["date"].max() - pd.Timedelta(weeks=weeks_split))].copy()
    expected_cls = (test_df[target] > 0).astype(int).reset_index(drop=True)
    y_test_cls_reset = y_test_cls.reset_index(drop=True)
    pd.testing.assert_series_equal(y_test_cls_reset, expected_cls, check_names=False)


# Test _evaluate_classifier
def test_evaluate_classifier_success():
    np.random.seed(42)
    X_test = pd.DataFrame({
        'item_id': ['T3L4D001'] * 100,
        'route': ['city_001 _ city_003'] * 100
    })
    y_test_cls = pd.Series([0, 1] * 50)

    # Mock classifier
    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.column_stack([
        np.random.uniform(0, 1, 100),
        np.random.uniform(0, 1, 100)
    ])

    threshold = 0.5
    precision, recall, f1, accuracy = _evaluate_classifier(X_test, y_test_cls, mock_classifier, threshold)

    # Check that metrics are returned
    assert isinstance(precision, (int, float))
    assert isinstance(recall, (int, float))
    assert isinstance(f1, (int, float))
    assert isinstance(accuracy, (int, float))

    # Check that metrics are in valid range
    assert 0 <= precision <= 1
    assert 0 <= recall <= 1
    assert 0 <= f1 <= 1
    assert 0 <= accuracy <= 1


def test_evaluate_classifier_perfect_predictions():
    X_test = pd.DataFrame({'item_id': ['T3L4D001'] * 100})
    y_test_cls = pd.Series([0, 1] * 50)

    # Mock classifier that predicts perfectly
    mock_classifier = MagicMock()
    proba = np.zeros((100, 2))
    proba[:, 1] = y_test_cls  # Perfect predictions
    mock_classifier.predict_proba.return_value = proba

    threshold = 0.5
    precision, recall, f1, accuracy = _evaluate_classifier(X_test, y_test_cls, mock_classifier, threshold)

    # Perfect predictions should give metrics = 1.0
    assert precision == 1.0
    assert recall == 1.0
    assert f1 == 1.0
    assert accuracy == 1.0


def test_evaluate_classifier_different_thresholds():
    X_test = pd.DataFrame({'item_id': ['T3L4D001'] * 100})
    y_test_cls = pd.Series([1] * 100)

    # Mock classifier
    mock_classifier = MagicMock()
    proba = np.column_stack([
        np.full(100, 0.4),
        np.full(100, 0.6)
    ])
    mock_classifier.predict_proba.return_value = proba

    # With low threshold, should predict all as 1
    precision1, recall1, f1_1, accuracy1 = _evaluate_classifier(X_test, y_test_cls, mock_classifier, 0.3)
    assert recall1 == 1.0

    # With high threshold, should predict all as 0
    precision2, recall2, f1_2, accuracy2 = _evaluate_classifier(X_test, y_test_cls, mock_classifier, 0.9)
    assert recall2 == 0.0


# Test _evaluate_regressor
def test_evaluate_regressor_success():
    np.random.seed(42)
    X_test = pd.DataFrame({
        'item_id': ['T3L4D001'] * 100,
        'route': ['city_001 _ city_003'] * 100
    })
    y_test = pd.Series(np.random.randint(0, 10, 100))

    # Mock models
    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.column_stack([
        np.random.uniform(0, 0.5, 100),
        np.random.uniform(0.5, 1, 100)
    ])

    mock_regressor = MagicMock()
    mock_regressor.predict.return_value = np.random.uniform(1, 5, 100)

    threshold = 0.5
    results_df, mae = _evaluate_regressor(X_test, y_test, mock_classifier, mock_regressor, threshold)

    # Check return types
    assert isinstance(results_df, pd.DataFrame)
    assert isinstance(mae, (int, float))

    # Check results_df structure
    assert 'fact' in results_df.columns
    assert 'predicted' in results_df.columns
    assert len(results_df) == len(y_test)

    # Check that MAE is non-negative
    assert mae >= 0


def test_evaluate_regressor_predicted_non_negative():
    X_test = pd.DataFrame({'item_id': ['T3L4D001'] * 10})
    y_test = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Mock models
    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.column_stack([
        np.zeros(10),
        np.ones(10)
    ])

    mock_regressor = MagicMock()
    mock_regressor.predict.return_value = np.array([-1, 0, 1, 2, 3, 4, 5, 6, 7, 8])

    threshold = 0.5
    results_df, mae = _evaluate_regressor(X_test, y_test, mock_classifier, mock_regressor, threshold)

    # Predictions should be clipped to non-negative
    assert all(results_df['predicted'] >= 0)


def test_evaluate_regressor_classifier_zeros():
    X_test = pd.DataFrame({'item_id': ['T3L4D001'] * 10})
    y_test = pd.Series([5] * 10)

    # Mock classifier that predicts all zeros
    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.column_stack([
        np.ones(10),
        np.zeros(10)
    ])

    mock_regressor = MagicMock()
    mock_regressor.predict.return_value = np.array([10] * 10)

    threshold = 0.5
    results_df, mae = _evaluate_regressor(X_test, y_test, mock_classifier, mock_regressor, threshold)

    # All predictions should be 0 (classifier predicted no sales)
    assert all(results_df['predicted'] == 0)


def test_evaluate_regressor_integer_predictions():
    X_test = pd.DataFrame({'item_id': ['T3L4D001'] * 10})
    y_test = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    # Mock models
    mock_classifier = MagicMock()
    mock_classifier.predict_proba.return_value = np.column_stack([
        np.zeros(10),
        np.ones(10)
    ])

    mock_regressor = MagicMock()
    mock_regressor.predict.return_value = np.array([1.7, 2.3, 3.9, 4.1, 5.5, 6.2, 7.8, 8.1, 9.6, 10.4])

    threshold = 0.5
    results_df, mae = _evaluate_regressor(X_test, y_test, mock_classifier, mock_regressor, threshold)

    # Predictions should be rounded to integers
    assert all(results_df['predicted'] == results_df['predicted'].astype(int))


# Test _evaluate_business_metrics
def test_evaluate_business_metrics_success():
    results_df = pd.DataFrame({
        'fact': [0, 1, 2, 3, 4, 5, 6, 7],
        'predicted': [0, 1, 3, 2, 5, 4, 8, 6]
    })

    metrics = _evaluate_business_metrics(results_df)

    # Check that all expected keys are present
    assert 'accurate' in metrics
    assert 'accurate_share' in metrics
    assert 'waste' in metrics
    assert 'waste_share' in metrics
    assert 'lost_sale' in metrics
    assert 'lost_sale_share' in metrics

    # Check types
    assert isinstance(metrics['accurate'], int)
    assert isinstance(metrics['accurate_share'], float)

    # Check that shares sum to 1
    assert abs(metrics['accurate_share'] + metrics['waste_share'] + metrics['lost_sale_share'] - 1.0) < 0.02


def test_evaluate_business_metrics_all_accurate():
    results_df = pd.DataFrame({
        'fact': [1, 2, 3, 4, 5],
        'predicted': [1, 2, 3, 4, 5]
    })

    metrics = _evaluate_business_metrics(results_df)

    assert metrics['accurate'] == 5
    assert metrics['accurate_share'] == 1.0
    assert metrics['waste'] == 0
    assert metrics['waste_share'] == 0.0
    assert metrics['lost_sale'] == 0
    assert metrics['lost_sale_share'] == 0.0


def test_evaluate_business_metrics_all_waste():
    results_df = pd.DataFrame({
        'fact': [1, 2, 3, 4, 5],
        'predicted': [2, 3, 4, 5, 6]
    })

    metrics = _evaluate_business_metrics(results_df)

    assert metrics['waste'] == 5
    assert metrics['waste_share'] == 1.0
    assert metrics['accurate'] == 0
    assert metrics['lost_sale'] == 0


def test_evaluate_business_metrics_all_lost_sale():
    results_df = pd.DataFrame({
        'fact': [2, 3, 4, 5, 6],
        'predicted': [1, 2, 3, 4, 5]
    })

    metrics = _evaluate_business_metrics(results_df)

    assert metrics['lost_sale'] == 5
    assert metrics['lost_sale_share'] == 1.0
    assert metrics['accurate'] == 0
    assert metrics['waste'] == 0


def test_evaluate_business_metrics_mixed():
    results_df = pd.DataFrame({
        'fact': [5, 5, 5, 5, 5, 5],
        'predicted': [5, 6, 4, 5, 7, 3]
    })

    metrics = _evaluate_business_metrics(results_df)

    # 2 accurate (index 0, 3), 2 waste (index 1, 4), 2 lost_sale (index 2, 5)
    assert metrics['accurate'] == 2
    assert metrics['waste'] == 2
    assert metrics['lost_sale'] == 2
    assert metrics['accurate_share'] == round(2/6, 2)
    assert metrics['waste_share'] == round(2/6, 2)
    assert metrics['lost_sale_share'] == round(2/6, 2)


# Test _evaluate_business_metrics_by_item
def test_evaluate_business_metrics_by_item_success():
    results_df = pd.DataFrame({
        'fact': [1, 2, 3, 1, 2, 3],
        'predicted': [1, 3, 2, 2, 2, 4]
    })
    item_ids = pd.Series(['T3L4D001', 'T3L4D001', 'T3L4D001', 'T3L4D002', 'T3L4D002', 'T3L4D002'])

    by_item = _evaluate_business_metrics_by_item(results_df, item_ids)

    # Check structure
    assert 'item_id' in by_item.columns
    assert 'accurate' in by_item.columns
    assert 'waste' in by_item.columns
    assert 'lost_sale' in by_item.columns

    # Check that we have 2 items
    assert len(by_item) == 2
    assert set(by_item['item_id']) == {'T3L4D001', 'T3L4D002'}


def test_evaluate_business_metrics_by_item_correct_counts():
    results_df = pd.DataFrame({
        'fact': [5, 5, 5],
        'predicted': [5, 6, 4]
    })
    item_ids = pd.Series(['T3L4D001', 'T3L4D001', 'T3L4D001'])

    by_item = _evaluate_business_metrics_by_item(results_df, item_ids)

    # For T3L4D001: 1 accurate, 1 waste, 1 lost_sale
    item_row = by_item[by_item['item_id'] == 'T3L4D001'].iloc[0]
    assert item_row['accurate'] == 1
    assert item_row['waste'] == 1
    assert item_row['lost_sale'] == 1


def test_evaluate_business_metrics_by_item_multiple_items():
    results_df = pd.DataFrame({
        'fact': [1, 1, 2, 2],
        'predicted': [1, 2, 2, 1]
    })
    item_ids = pd.Series(['T3L4D001', 'T3L4D001', 'T3L4D002', 'T3L4D002'])

    by_item = _evaluate_business_metrics_by_item(results_df, item_ids)

    # T3L4D001: 1 accurate, 1 waste, 0 lost_sale
    item1 = by_item[by_item['item_id'] == 'T3L4D001'].iloc[0]
    assert item1['accurate'] == 1
    assert item1['waste'] == 1
    assert item1['lost_sale'] == 0

    # T3L4D002: 1 accurate, 0 waste, 1 lost_sale
    item2 = by_item[by_item['item_id'] == 'T3L4D002'].iloc[0]
    assert item2['accurate'] == 1
    assert item2['waste'] == 0
    assert item2['lost_sale'] == 1


# Test _check_degradation
@patch('forecasting.model.model_evaluation.evaluate.read_sql')
def test_check_degradation_no_previous(mock_read_sql):
    # No previous model
    mock_read_sql.return_value = pd.DataFrame()

    is_degraded, message = _check_degradation(0.85, 0.1)

    assert is_degraded is False
    assert message == "No previous model to compare"


@patch('forecasting.model.model_evaluation.evaluate.read_sql')
def test_check_degradation_acceptable(mock_read_sql):
    # Previous accuracy was 0.85, current is 0.84 (1.2% drop)
    mock_read_sql.return_value = pd.DataFrame({'metric_value': [0.85]})

    is_degraded, message = _check_degradation(0.84, 0.1)

    assert is_degraded is False
    assert message == "Performance acceptable"


@patch('forecasting.model.model_evaluation.evaluate.read_sql')
def test_check_degradation_detected(mock_read_sql):
    # Previous accuracy was 0.90, current is 0.80 (11.1% drop, threshold is 10%)
    mock_read_sql.return_value = pd.DataFrame({'metric_value': [0.90]})

    is_degraded, message = _check_degradation(0.80, 0.1)

    assert is_degraded is True
    assert "dropped" in message.lower()
    assert "0.900" in message
    assert "0.800" in message


@patch('forecasting.model.model_evaluation.evaluate.read_sql')
def test_check_degradation_exception_handling(mock_read_sql):
    # Database error
    mock_read_sql.side_effect = Exception("Database error")

    is_degraded, message = _check_degradation(0.85, 0.1)

    assert is_degraded is False
    assert message == "Degradation check skipped"


@patch('forecasting.model.model_evaluation.evaluate.read_sql')
def test_check_degradation_improvement(mock_read_sql):
    # Model improved (previous 0.80, current 0.90)
    mock_read_sql.return_value = pd.DataFrame({'metric_value': [0.80]})

    is_degraded, message = _check_degradation(0.90, 0.1)

    assert is_degraded is False
    assert message == "Performance acceptable"


@patch('forecasting.model.model_evaluation.evaluate.read_sql')
def test_check_degradation_exact_threshold(mock_read_sql):
    # Exactly at threshold (previous 0.90, current 0.81, exactly 10% drop)
    mock_read_sql.return_value = pd.DataFrame({'metric_value': [0.90]})

    is_degraded, message = _check_degradation(0.81, 0.1)

    # Should NOT be degraded (drop = threshold, not > threshold)
    assert is_degraded is False
    assert message == "Performance acceptable"
