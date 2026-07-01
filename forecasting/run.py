from forecasting.data_preparation.data_ingestion import MockSource, DBSource
from forecasting.data_preparation.data_cleanup.data_cleaning import clean_data
from forecasting.data_preparation.feature_engineering.feature_engineering import build_features
from forecasting.data_preparation.data_validation.data_validating import validate_data
from forecasting.model.model_building.train import train_model
from forecasting.model.model_evaluation.evaluate import evaluate

from forecasting.utils.config_handler import return_config, FORECASTING_PATH
from forecasting.utils.exceptions import ModelDegradationError


def run_pipeline():
    config = return_config()
    data_source_type = config["data_preparation"]["data_ingestion"]["data_source"]

    if data_source_type == "mock":
        source = MockSource(path=FORECASTING_PATH/"interim_files/raw_data_df.parquet")
    else:
        source = DBSource()


    # STEP 1: reading data from its source
    print(f"STEP 1: DATA IS FETCHD FROM THE SOURCE {data_source_type}")
    df = source.fetch()

    # STEP 2: data cleanup
    df = clean_data(df)
    print("STEP 2: DATA IS CLEANED")

    # STEP 3: feature engineering
    print("STEP 3: FEATURE ENGINEERING")
    df = build_features(df)


    # STEP 4: data validation
    print("STEP 4: DATA VALIDATION")
    validate_data(df)


    # STEP 5: train model
    print("STEP 5: MODEL TRAINING")
    classifier, regressor, model_latest_version = train_model(df)


    # STEP 6: evaluate
    print("STEP 6: MODEL EVALUATION")
    # missed sales
    low_missed_sales_threshold = "low_missed_sales"
    low_missed_sales_threshold_value = config["model"]["catboost"]["low_missed_sales"]
    try:
        evaluate(df=df,
             classifier=classifier,
             regressor=regressor,
             threshold=low_missed_sales_threshold_value,
             threshold_type=low_missed_sales_threshold,
             model_version=model_latest_version)
    except ModelDegradationError as e:
        print(f"Pipeline stopped: {e}")
        print("Human approval required before deploying new model version")
        raise

    # wastage
    low_wast_threshold = "low_wastage"
    low_wast_threshold_value = config["model"]["catboost"]["low_wastage"]
    try:
        evaluate(df=df,
             classifier=classifier,
             regressor=regressor,
             threshold=low_wast_threshold_value,
             threshold_type=low_wast_threshold,
             model_version=model_latest_version)
    except ModelDegradationError as e:
        print(f"Pipeline stopped: {e}")
        print("Human approval required before deploying new model version")
        raise
    print("\nPIPELINE COMPLETED")



if __name__ == "__main__":
    run_pipeline()


