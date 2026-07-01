from catboost import CatBoostClassifier, CatBoostRegressor

from forecasting.model.model_registry.handle_model import save_model
from forecasting.utils.config_handler import return_config
import pandas as pd



def _prepare_dataset(df: pd.DataFrame, feature_list: list, target: str) -> pd.DataFrame:
    feature_list = feature_list + [target]
    return df[feature_list]


def _split_train_test_set(df: pd.DataFrame, config_set_split: str) -> pd.DataFrame:
    df = df.sort_values(by="date", ascending=True)
    test_set_cutoff = df["date"].max() - pd.Timedelta(weeks=int(config_set_split))
    train_df: pd.DataFrame = df[df["date"] <= test_set_cutoff]
    test_df: pd.DataFrame = df[df["date"] > test_set_cutoff]
    print(f"Train DF size: {train_df.shape}")
    print(f"Test DF size: {test_df.shape}")
    return train_df


def _train_classifier(train_df: pd.DataFrame, classifier_config: dict,
                      features: list, target: str,
                      cat_features: list) -> CatBoostClassifier:
    X_train = train_df[features]
    y_train_cls = (train_df[target] > 0).astype(int)

    classifier = CatBoostClassifier(
        iterations=classifier_config["iterations"],
        learning_rate=classifier_config["learning_rate"],
        depth=classifier_config["depth"],
        cat_features=cat_features,
        eval_metric='F1',
        random_seed=classifier_config["random_seed"],
        verbose=100
    )

    classifier.fit(X_train, y_train_cls)

    return classifier


def _train_regression(train_df: pd.DataFrame, regressor_config: dict,
                      features: list, target: str,
                      cat_features: list) -> CatBoostRegressor:
    train_df = train_df[train_df[target] > 0]
    X_train = train_df[features]
    y_train = train_df[target]

    regressor = CatBoostRegressor(
        iterations=regressor_config["iterations"],
        learning_rate=regressor_config["learning_rate"],
        depth=regressor_config["depth"],
        cat_features=cat_features,
        eval_metric='MAE',
        random_seed=regressor_config["random_seed"],
        verbose=100
    )
    regressor.fit(X_train, y_train)

    return regressor


def train_model(df: pd.DataFrame) -> tuple:
    df = df.copy()
    config = return_config()

    # splitting DataFrame to test and train
    config_set_split = config["model"]["test_split_by_weeks"]
    train_df = _split_train_test_set(df, config_set_split)

    # leaving only needed columns
    categorical_features = config["model"]["model_features"]["categorical"]
    numerical_features = config["model"]["model_features"]["numerical"]
    target = config["model"]["model_features"]["target"]
    all_features = categorical_features + numerical_features

    df = _prepare_dataset(df, all_features, target)

    # training classifier
    config_classifier = config["model"]["catboost"]["classifier"]
    classifier = _train_classifier(train_df, config_classifier, all_features, target, categorical_features)

    # training regression
    config_regressor = config["model"]["catboost"]["regressor"]
    regressor = _train_regression(train_df, config_regressor, all_features, target, categorical_features)

    # saving model
    latest_version = save_model(classifier, regressor)

    return classifier, regressor, latest_version