import pandas as pd
from forecasting.utils.exceptions import ValidationError
from forecasting.utils.config_handler import return_config


def _check_columns(df: pd.DataFrame, required_columns: list) -> None:

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValidationError(f"Missing columns: {missing}")

def _check_nan_values(df: pd.DataFrame, required_columns: list) -> None:
    na_values = df[required_columns].isna().sum()
    columns_with_nan = na_values[na_values > 0]
    if len(columns_with_nan) > 0:
        raise ValidationError(f"NaN values are present in: {columns_with_nan.to_dict()}")

def _check_target(df: pd.DataFrame, target: str) -> None:
    if not pd.api.types.is_numeric_dtype(df[target]):
        raise ValidationError(f"Target '{target}' is not numeric")
    if (df[target] < 0).any():
        raise ValidationError(f"Target '{target}' has negative values")

def _check_hist_avg(df: pd.DataFrame, hist_avg_levels: int) -> None:
    if df["hist_avg"].isna().any():
        raise ValidationError("hist_avg column contains NaN values")

    allowed_values = list(range(1, hist_avg_levels + 1))
    invalid = ~df["hist_level_used"].isin(allowed_values)
    if invalid.any():
        raise ValidationError(f"Invalid values in hist_level_used: "
                              f"{df.loc[invalid, 'hist_level_used'].unique().tolist()}")

def _check_dates(df: pd.DataFrame, start_date: str, end_date: str) -> None:
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    df_min = df["date"].min()
    df_max = df["date"].max()

    if df_min < start_date or df_min > end_date:
        raise ValidationError(f"Dataset start date {df_min.date()} is outside expected range "
                              f"[{start_date.date()} : {end_date.date()}]")
    if df_max > end_date or df_max < start_date:
        raise ValidationError(f"Dataset end date {df_max.date()} is outside expected range "
                              f"[{start_date.date()} : {end_date.date()}]")

def validate_data(df: pd.DataFrame) -> None:
    config = return_config()
    validation_config = config["data_preparation"]["data_validation"]

    _check_columns(df, validation_config["required_columns"])
    _check_nan_values(df, validation_config["required_columns"])
    _check_target(df, validation_config["target"])
    _check_hist_avg(df, validation_config["hist_avg_levels"])
    _check_dates(df, validation_config["start_date"], validation_config["end_date"])