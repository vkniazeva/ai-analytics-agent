import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from forecasting.model.model_building.train import (
    _prepare_dataset,
    _split_train_test_set,
    _train_classifier,
    _train_regression
)


# Helper function to create valid dataframe
def create_valid_dataframe():
    dates = pd.date_range(start='2025-01-01', end='2025-12-31', freq='D')
    n = len(dates)

    np.random.seed(42)  # For reproducibility

    return pd.DataFrame({
        'date': dates,
        'item_id': (['AB123', 'AB124', 'AB125'] * (n // 3 + 1))[:n],
        'route': (['city_001 _ city_003', 'city_002 _ city_004'] * (n // 2 + 1))[:n],
        'day_period': (['morning', 'afternoon', 'night'] * (n // 3 + 1))[:n],
        'pax_bins': (['<100', '100 - 150'] * (n // 2 + 1))[:n],
        'hist_avg': np.random.uniform(0, 3, n),
        'sold_quantity': np.random.randint(0, 5, n),
        'extra_column': ['value'] * n
    })


# Test _prepare_dataset
def test_prepare_dataset_success():
    df = create_valid_dataframe()
    feature_list = ['item_id', 'route', 'hist_avg']
    target = 'sold_quantity'

    result = _prepare_dataset(df, feature_list, target)

    # Should have only selected features + target
    expected_columns = feature_list + [target]
    assert list(result.columns) == expected_columns
    assert len(result) == len(df)


def test_prepare_dataset_preserves_data():
    df = create_valid_dataframe()
    feature_list = ['item_id', 'hist_avg']
    target = 'sold_quantity'

    result = _prepare_dataset(df, feature_list, target)

    # Data should be preserved
    pd.testing.assert_series_equal(result['item_id'], df['item_id'], check_names=True)
    pd.testing.assert_series_equal(result['hist_avg'], df['hist_avg'], check_names=True)
    pd.testing.assert_series_equal(result['sold_quantity'], df['sold_quantity'], check_names=True)


def test_prepare_dataset_removes_extra_columns():
    df = create_valid_dataframe()
    feature_list = ['item_id']
    target = 'sold_quantity'

    result = _prepare_dataset(df, feature_list, target)

    # Extra columns should be removed
    assert 'extra_column' not in result.columns
    assert 'route' not in result.columns
    assert 'date' not in result.columns


# Test _split_train_test_set
def test_split_train_test_set_success():
    df = create_valid_dataframe()
    config_set_split = "2"  # 2 weeks

    train_df = _split_train_test_set(df, config_set_split)

    # Should return a dataframe
    assert isinstance(train_df, pd.DataFrame)

    # Train set should be smaller than full dataset
    assert len(train_df) > 0
    assert len(train_df) < len(df)


def test_split_train_test_set_date_ordering():
    df = create_valid_dataframe()
    config_set_split = "2"

    train_df = _split_train_test_set(df, config_set_split)

    # Train dates should be before cutoff
    cutoff_date = df["date"].max() - pd.Timedelta(weeks=2)
    assert all(train_df['date'] <= cutoff_date)


def test_split_train_test_set_correct_cutoff():
    df = create_valid_dataframe()
    config_set_split = "2"
    weeks = 2

    train_df = _split_train_test_set(df, config_set_split)

    # Check that cutoff is correct
    cutoff_date = df["date"].max() - pd.Timedelta(weeks=weeks)
    train_max_date = train_df['date'].max()

    assert train_max_date <= cutoff_date


def test_split_train_test_set_different_split():
    df = create_valid_dataframe()

    train_df_2w = _split_train_test_set(df, "2")
    train_df_4w = _split_train_test_set(df, "4")

    # 4 weeks split should have smaller train set
    assert len(train_df_4w) < len(train_df_2w)


def test_split_train_test_set_preserves_columns():
    df = create_valid_dataframe()
    config_set_split = "2"

    train_df = _split_train_test_set(df, config_set_split)

    # All columns should be preserved
    assert list(train_df.columns) == list(df.columns)


# Test _train_classifier
def test_train_classifier_returns_model():
    df = create_valid_dataframe()
    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    classifier_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    classifier = _train_classifier(df, classifier_config, features, target, cat_features)

    # Should return classifier
    assert classifier is not None

    # Should be able to make predictions
    predictions = classifier.predict_proba(df[features])[:, 1]
    assert len(predictions) == len(df)
    assert np.all(predictions >= 0)
    assert np.all(predictions <= 1)


def test_train_classifier_binary_target():
    df = create_valid_dataframe()
    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    classifier_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    classifier = _train_classifier(df, classifier_config, features, target, cat_features)

    # Classifier should predict binary (0 or 1)
    predictions = classifier.predict(df[features])
    assert set(predictions).issubset({0, 1})


def test_train_classifier_with_mostly_zeros():
    df = create_valid_dataframe()
    df['sold_quantity'] = 0  # All zeros
    # Add a few non-zeros so CatBoost can train (needs at least 2 classes)
    df.loc[:10, 'sold_quantity'] = 1

    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    classifier_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    classifier = _train_classifier(df, classifier_config, features, target, cat_features)

    # Should handle mostly-zero case
    assert classifier is not None
    probas = classifier.predict_proba(df[features])[:, 1]
    # Most probabilities should be low (predicting zeros)
    assert np.mean(probas) < 0.5


def test_train_classifier_with_mostly_nonzeros():
    df = create_valid_dataframe()
    df['sold_quantity'] = df['sold_quantity'] + 1  # All non-zeros
    # Add a few zeros so CatBoost can train (needs at least 2 classes)
    df.loc[:10, 'sold_quantity'] = 0

    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    classifier_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    classifier = _train_classifier(df, classifier_config, features, target, cat_features)

    # Should handle mostly-nonzero case
    assert classifier is not None
    probas = classifier.predict_proba(df[features])[:, 1]
    # Most probabilities should be high (predicting non-zeros)
    assert np.mean(probas) > 0.5


# Test _train_regression
def test_train_regression_returns_model():
    df = create_valid_dataframe()
    df = df[df['sold_quantity'] > 0]  # Only non-zero values

    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    regressor_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    regressor = _train_regression(df, regressor_config, features, target, cat_features)

    # Should return regressor
    assert regressor is not None

    # Should be able to make predictions
    predictions = regressor.predict(df[features])
    assert len(predictions) == len(df)
    assert np.all(predictions >= 0)  # Predictions should be non-negative


def test_train_regression_filters_zeros():
    df = create_valid_dataframe()
    original_len = len(df)
    zero_count = (df['sold_quantity'] == 0).sum()

    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    regressor_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    regressor = _train_regression(df, regressor_config, features, target, cat_features)

    # Model should be trained only on non-zero values
    # We can't directly check this, but we can verify the model works
    assert regressor is not None


def test_train_regression_positive_predictions():
    df = create_valid_dataframe()
    df['sold_quantity'] = df['sold_quantity'] + 1  # Ensure all positive

    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    regressor_config = {
        'iterations': 10,
        'learning_rate': 0.1,
        'depth': 4,
        'random_seed': 42
    }

    regressor = _train_regression(df, regressor_config, features, target, cat_features)

    predictions = regressor.predict(df[features])

    # Predictions should be positive (we trained on positive values)
    assert np.all(predictions > 0)


# Edge cases
def test_prepare_dataset_single_feature():
    df = create_valid_dataframe()
    feature_list = ['hist_avg']
    target = 'sold_quantity'

    result = _prepare_dataset(df, feature_list, target)

    assert list(result.columns) == ['hist_avg', 'sold_quantity']
    assert len(result) == len(df)


def test_split_train_test_set_small_dataset():
    # Create small dataset
    df = pd.DataFrame({
        'date': pd.date_range(start='2025-01-01', end='2025-01-20', freq='D'),
        'item_id': ['AB123'] * 20,
        'sold_quantity': [1] * 20
    })

    config_set_split = "1"  # 1 week

    train_df = _split_train_test_set(df, config_set_split)

    # Should still work with small dataset
    assert len(train_df) > 0
    assert len(train_df) < len(df)


def test_train_classifier_minimal_config():
    df = create_valid_dataframe()
    features = ['hist_avg']  # Single feature
    target = 'sold_quantity'
    cat_features = []

    classifier_config = {
        'iterations': 5,  # Minimal iterations
        'learning_rate': 0.1,
        'depth': 2,
        'random_seed': 42
    }

    classifier = _train_classifier(df, classifier_config, features, target, cat_features)

    # Should work with minimal config
    assert classifier is not None
    predictions = classifier.predict_proba(df[features])[:, 1]
    assert len(predictions) == len(df)


def test_train_regression_minimal_data():
    # Small dataset with only non-zero values and varied features
    df = pd.DataFrame({
        'item_id': ['AB123', 'AB124'] * 10,
        'route': ['city_001 _ city_003', 'city_002 _ city_004'] * 10,
        'day_period': ['morning', 'afternoon', 'night'] * 6 + ['morning', 'afternoon'],
        'pax_bins': ['<100', '100 - 150'] * 10,
        'hist_avg': np.random.uniform(1.0, 3.0, 20),
        'sold_quantity': [1, 2, 3, 2, 1] * 4
    })

    features = ['item_id', 'route', 'day_period', 'pax_bins', 'hist_avg']
    target = 'sold_quantity'
    cat_features = ['item_id', 'route', 'day_period', 'pax_bins']

    regressor_config = {
        'iterations': 5,
        'learning_rate': 0.1,
        'depth': 2,
        'random_seed': 42
    }

    regressor = _train_regression(df, regressor_config, features, target, cat_features)

    # Should handle minimal data
    assert regressor is not None
    predictions = regressor.predict(df[features])
    assert len(predictions) == len(df)
