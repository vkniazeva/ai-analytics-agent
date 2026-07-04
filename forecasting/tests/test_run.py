import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, call
from catboost import CatBoostClassifier, CatBoostRegressor

from forecasting.run import run_pipeline


@pytest.fixture
def mock_config():
    return {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "mock"
            }
        },
        "model": {
            "catboost": {
                "low_missed_sales": 0.4,
                "low_wastage": 0.7
            }
        }
    }


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        'item_id': ['T3L4D001', 'T3L4D002'],
        'sold_quantity': [5, 3],
        'date': pd.to_datetime(['2025-01-01', '2025-01-02'])
    })


@pytest.fixture
def mock_models():
    classifier = MagicMock(spec=CatBoostClassifier)
    regressor = MagicMock(spec=CatBoostRegressor)
    return classifier, regressor


# Test run_pipeline with mock source
@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_mock_source(mock_evaluate, mock_train, mock_validate,
                                   mock_build_features, mock_clean, mock_mock_source,
                                   mock_config_fn, mock_config, sample_dataframe, mock_models):
    mock_config_fn.return_value = mock_config

    # Mock data source
    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_mock_source.return_value = mock_source_instance

    # Mock processing steps
    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe
    mock_validate.return_value = None

    # Mock training
    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Verify MockSource was instantiated
    mock_mock_source.assert_called_once()

    # Verify all steps were called in order
    mock_source_instance.fetch.assert_called_once()
    mock_clean.assert_called_once()
    mock_build_features.assert_called_once()
    mock_validate.assert_called_once()
    mock_train.assert_called_once()

    # Verify evaluate was called twice (for both thresholds)
    assert mock_evaluate.call_count == 2


@patch('forecasting.run.return_config')
@patch('forecasting.run.DBSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_db_source(mock_evaluate, mock_train, mock_validate,
                                 mock_build_features, mock_clean, mock_db_source,
                                 mock_config_fn, sample_dataframe, mock_models):
    # Configure for database source
    config = {
        "data_preparation": {
            "data_ingestion": {
                "data_source": "database"
            }
        },
        "model": {
            "catboost": {
                "low_missed_sales": 0.4,
                "low_wastage": 0.7
            }
        }
    }
    mock_config_fn.return_value = config

    # Mock data source
    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_db_source.return_value = mock_source_instance

    # Mock processing steps
    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe
    mock_validate.return_value = None

    # Mock training
    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Verify DBSource was instantiated
    mock_db_source.assert_called_once()
    mock_source_instance.fetch.assert_called_once()


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_data_flow(mock_evaluate, mock_train, mock_validate,
                                 mock_build_features, mock_clean, mock_mock_source,
                                 mock_config_fn, mock_config, mock_models):
    mock_config_fn.return_value = mock_config

    # Create different dataframes for each step to verify data flow
    raw_df = pd.DataFrame({'item_id': ['T3L4D001']})
    cleaned_df = pd.DataFrame({'item_id': ['T3L4D001'], 'cleaned': [True]})
    featured_df = pd.DataFrame({'item_id': ['T3L4D001'], 'cleaned': [True], 'features': [1]})

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = raw_df
    mock_mock_source.return_value = mock_source_instance

    mock_clean.return_value = cleaned_df
    mock_build_features.return_value = featured_df

    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Verify data flows through pipeline correctly
    mock_clean.assert_called_once_with(raw_df)
    mock_build_features.assert_called_once_with(cleaned_df)
    mock_validate.assert_called_once_with(featured_df)
    mock_train.assert_called_once_with(featured_df)


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_evaluate_calls(mock_evaluate, mock_train, mock_validate,
                                      mock_build_features, mock_clean, mock_mock_source,
                                      mock_config_fn, mock_config, sample_dataframe, mock_models):
    mock_config_fn.return_value = mock_config

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_mock_source.return_value = mock_source_instance

    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe

    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Verify evaluate was called twice with correct parameters
    assert mock_evaluate.call_count == 2

    # Check first evaluate call (low_missed_sales)
    first_call = mock_evaluate.call_args_list[0]
    assert first_call[1]['threshold'] == 0.4
    assert first_call[1]['threshold_type'] == 'low_missed_sales'
    assert first_call[1]['model_version'] == 'v1'

    # Check second evaluate call (low_wastage)
    second_call = mock_evaluate.call_args_list[1]
    assert second_call[1]['threshold'] == 0.7
    assert second_call[1]['threshold_type'] == 'low_wastage'
    assert second_call[1]['model_version'] == 'v1'


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_uses_same_models_for_evaluation(mock_evaluate, mock_train, mock_validate,
                                                       mock_build_features, mock_clean,
                                                       mock_mock_source, mock_config_fn,
                                                       mock_config, sample_dataframe, mock_models):
    mock_config_fn.return_value = mock_config

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_mock_source.return_value = mock_source_instance

    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe

    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Both evaluate calls should use same classifier and regressor
    first_call_classifier = mock_evaluate.call_args_list[0][1]['classifier']
    first_call_regressor = mock_evaluate.call_args_list[0][1]['regressor']

    second_call_classifier = mock_evaluate.call_args_list[1][1]['classifier']
    second_call_regressor = mock_evaluate.call_args_list[1][1]['regressor']

    assert first_call_classifier is second_call_classifier
    assert first_call_regressor is second_call_regressor


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_prints_steps(mock_evaluate, mock_train, mock_validate,
                                    mock_build_features, mock_clean, mock_mock_source,
                                    mock_config_fn, mock_config, sample_dataframe,
                                    mock_models, capsys):
    mock_config_fn.return_value = mock_config

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_mock_source.return_value = mock_source_instance

    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe

    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Capture printed output
    captured = capsys.readouterr()

    # Verify all steps are printed
    assert "STEP 1: DATA IS FETCHD FROM THE SOURCE" in captured.out
    assert "STEP 2: DATA IS CLEANED" in captured.out
    assert "STEP 3: FEATURE ENGINEERING" in captured.out
    assert "STEP 4: DATA VALIDATION" in captured.out
    assert "STEP 5: MODEL TRAINING" in captured.out
    assert "STEP 6: MODEL EVALUATION" in captured.out
    assert "PIPELINE COMPLETED" in captured.out


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_uses_same_dataframe_for_evaluation(mock_evaluate, mock_train, mock_validate,
                                                          mock_build_features, mock_clean,
                                                          mock_mock_source, mock_config_fn,
                                                          mock_config, sample_dataframe, mock_models):
    mock_config_fn.return_value = mock_config

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_mock_source.return_value = mock_source_instance

    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe

    classifier, regressor = mock_models
    mock_train.return_value = (classifier, regressor, "v1")

    # Run pipeline
    run_pipeline()

    # Both evaluate calls should use the same dataframe
    first_call_df = mock_evaluate.call_args_list[0][1]['df']
    second_call_df = mock_evaluate.call_args_list[1][1]['df']

    assert first_call_df is second_call_df


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_exception_in_data_fetch(mock_evaluate, mock_train, mock_validate,
                                               mock_build_features, mock_clean, mock_mock_source,
                                               mock_config_fn, mock_config):
    mock_config_fn.return_value = mock_config

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.side_effect = Exception("Data fetch failed")
    mock_mock_source.return_value = mock_source_instance

    # Should raise exception
    with pytest.raises(Exception, match="Data fetch failed"):
        run_pipeline()

    # Verify subsequent steps were not called
    mock_clean.assert_not_called()
    mock_build_features.assert_not_called()
    mock_validate.assert_not_called()
    mock_train.assert_not_called()
    mock_evaluate.assert_not_called()


@patch('forecasting.run.return_config')
@patch('forecasting.run.MockSource')
@patch('forecasting.run.clean_data')
@patch('forecasting.run.build_features')
@patch('forecasting.run.validate_data')
@patch('forecasting.run.train_model')
@patch('forecasting.run.evaluate')
def test_run_pipeline_exception_in_validation(mock_evaluate, mock_train, mock_validate,
                                               mock_build_features, mock_clean, mock_mock_source,
                                               mock_config_fn, mock_config, sample_dataframe):
    mock_config_fn.return_value = mock_config

    mock_source_instance = MagicMock()
    mock_source_instance.fetch.return_value = sample_dataframe
    mock_mock_source.return_value = mock_source_instance

    mock_clean.return_value = sample_dataframe
    mock_build_features.return_value = sample_dataframe
    mock_validate.side_effect = Exception("Validation failed")

    # Should raise exception
    with pytest.raises(Exception, match="Validation failed"):
        run_pipeline()

    # Verify previous steps were called
    mock_clean.assert_called_once()
    mock_build_features.assert_called_once()
    mock_validate.assert_called_once()

    # Verify subsequent steps were not called
    mock_train.assert_not_called()
    mock_evaluate.assert_not_called()
