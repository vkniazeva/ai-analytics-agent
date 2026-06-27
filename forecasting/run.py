from forecasting.data_preparation.data_ingestion import MockSource, DBSource
from forecasting.data_preparation.data_cleanup.data_cleaning import clean_data
from forecasting.data_preparation.feature_engineering.feature_engineering import build_features
from forecasting.data_preparation.data_validation.data_validating import validate_data

from forecasting.utils.config_handler import return_config, FORECASTING_PATH

config = return_config()
data_source_type = config["data_preparation"]["data_ingestion"]["data_source"]

if data_source_type == "mock":
    source = MockSource(path=FORECASTING_PATH/"interim_files/raw_data_df.parquet")
else:
    source = DBSource()


# STEP 1: reading data from its source
df = source.fetch()
print(f"STEP 1: DATA IS FETCHD FROM THE SOURCE {data_source_type}")

# STEP 2: data cleanup
df = clean_data(df)
print("STEP 2: DATA IS CLEANED")

# STEP 3: feature engineering
df = build_features(df)
print("STEP 3: FEATURE ENGINEERING")
print(df.columns)

# STEP 4: data validation
validate_data(df)
print("STEP 4: DATA VALIDATION")


