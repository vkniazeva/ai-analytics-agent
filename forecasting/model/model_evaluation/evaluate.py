import pandas as pd
import numpy as np
import logging
from sklearn.metrics import precision_score, recall_score, f1_score, mean_absolute_error
from catboost import CatBoostClassifier, CatBoostRegressor

from forecasting.utils.config_handler import return_config
from forecasting.utils.database import write_sql
from forecasting.utils.exceptions import ModelDegradationError
from forecasting.utils.database import read_sql

logger = logging.getLogger(__name__)


def _create_test_set(df: pd.DataFrame, weeks_split: int, features: list, target: str) -> tuple:
    cutoff_date = df["date"].max() - pd.Timedelta(weeks=int(weeks_split))
    test_df = df[df["date"] > cutoff_date].copy()
    X_test = test_df[features]
    y_test_cls = (test_df[target] > 0).astype(int)
    y_test = test_df[target]
    item_ids = test_df["item_id"].reset_index(drop=True)
    return X_test, y_test_cls, y_test, item_ids


def _evaluate_classifier(X_test: pd.DataFrame, y_test_cls: pd.Series,
                          classifier: CatBoostClassifier, threshold: float) -> tuple:
    proba = classifier.predict_proba(X_test)[:, 1]
    predicted = (proba >= threshold).astype(int)
    precision = precision_score(y_test_cls, predicted)
    recall = recall_score(y_test_cls, predicted)
    f1 = f1_score(y_test_cls, predicted)
    accuracy = (y_test_cls == predicted).mean()
    return precision, recall, f1, accuracy


def _evaluate_regressor(X_test: pd.DataFrame, y_test: pd.Series, classifier: CatBoostClassifier,
                         regressor: CatBoostRegressor, threshold: float) -> tuple[pd.DataFrame, float]:
    # Round and clip predictions (sold_quantity must be non-negative integer)
    cls_proba = classifier.predict_proba(X_test)[:, 1]
    cls_pred = (cls_proba >= threshold).astype(int)

    reg_pred = regressor.predict(X_test).round().clip(0).astype(int)
    final_pred = np.where(cls_pred == 0, 0, reg_pred)

    results_df = pd.DataFrame({
        "fact": y_test.values,
        "predicted": final_pred
    })
    mae = mean_absolute_error(results_df["fact"], results_df["predicted"])
    return results_df, mae


def _evaluate_business_metrics(results_df: pd.DataFrame) -> dict:
    results_df = results_df.copy()
    results_df["diff"] = results_df["predicted"] - results_df["fact"]
    total = len(results_df)

    accurate = (results_df["diff"] == 0).sum()
    waste = (results_df["diff"] > 0).sum()
    lost_sale = (results_df["diff"] < 0).sum()

    return {
        "accurate": int(accurate),
        "accurate_share": round(accurate / total, 2),
        "waste": int(waste),
        "waste_share": round(waste / total, 2),
        "lost_sale": int(lost_sale),
        "lost_sale_share": round(lost_sale / total, 2)
    }


def _evaluate_business_metrics_by_item(results_df: pd.DataFrame,
                                        item_ids: pd.Series) -> pd.DataFrame:
    results_df = results_df.copy()
    results_df["item_id"] = item_ids.values
    results_df["diff"] = results_df["predicted"] - results_df["fact"]

    results_df["accurate"] = (results_df["diff"] == 0).astype(int)
    results_df["waste"] = (results_df["diff"] > 0).astype(int)
    results_df["lost_sale"] = (results_df["diff"] < 0).astype(int)

    by_item = results_df.groupby("item_id")[["accurate", "waste", "lost_sale"]].sum().reset_index()
    return by_item


def _check_degradation(accuracy: float, degradation_threshold: float, table_name=None) -> tuple[bool, str]:
    """
    Check if model performance degraded compared to previous version.

    Returns:
        tuple: (is_degraded: bool, message: str)
    """
    query = """
        SELECT metric_value
        FROM forecasting.model_metrics
        WHERE metric_name = 'accuracy'
        ORDER BY run_id DESC
        LIMIT 1
    """
    try:
        previous = read_sql(query, table_name)
        if len(previous) == 0:
            return False, "No previous model to compare"

        previous_accuracy = previous["metric_value"].iloc[0]
        drop = (previous_accuracy - accuracy) / previous_accuracy

        if drop > degradation_threshold:
            message = (
                f"Model accuracy dropped by {round(drop * 100, 1)}% "
                f"(previous: {previous_accuracy:.3f}, current: {accuracy:.3f})"
            )
            return True, message

        return False, "Performance acceptable"

    except Exception as e:
        logger.warning(f"Could not check degradation: {e}")
        return False, "Degradation check skipped"


def evaluate(df: pd.DataFrame, classifier: CatBoostClassifier,
             regressor: CatBoostRegressor, threshold: float,
             threshold_type: str, model_version: str) -> None:

    config = return_config()
    weeks_split = config["model"]["test_split_by_weeks"]
    cat_features = config["model"]["model_features"]["categorical"]
    num_features = config["model"]["model_features"]["numerical"]
    target = config["model"]["model_features"]["target"]
    features = cat_features + num_features
    degradation_threshold = config["model"]["degradation_threshold"]

    # test set
    X_test, y_test_cls, y_test, item_ids = _create_test_set(df, weeks_split, features, target)

    # classifier
    precision, recall, f1, accuracy = _evaluate_classifier(X_test, y_test_cls, classifier, threshold)

    # regressor
    reg_results_df, mae = _evaluate_regressor(X_test, y_test, classifier, regressor, threshold)

    # business metrics
    business_metrics = _evaluate_business_metrics(reg_results_df)
    business_metrics_by_item = _evaluate_business_metrics_by_item(reg_results_df, item_ids)


    # degradation check
    versions_table_name = "model_runs"
    is_degraded, degradation_message = _check_degradation(accuracy, degradation_threshold, versions_table_name)

    if is_degraded:
        # Log critical alert
        logger.critical("="*80)
        logger.critical("MODEL PERFORMANCE DEGRADATION DETECTED")
        logger.critical(degradation_message)
        logger.critical("="*80)
        logger.critical("Actions taken:")
        logger.critical("  - New model NOT saved to database")
        logger.critical("  - Old model remains active")
        logger.critical("  - Metrics logged below for analysis")
        logger.critical("="*80)

        # Log metrics for manual review
        logger.info(f"Model Metrics (threshold={threshold_type}):")
        logger.info(f"  Classifier - Accuracy: {accuracy:.3f}, Precision: {precision:.3f}, Recall: {recall:.3f}, F1: {f1:.3f}")
        logger.info(f"  Regressor  - MAE: {mae:.3f}")
        logger.info(f"  Business   - Accurate: {business_metrics['accurate_share']*100:.1f}%, "
                   f"Waste: {business_metrics['waste_share']*100:.1f}%, "
                   f"Lost Sale: {business_metrics['lost_sale_share']*100:.1f}%")

        logger.warning("Manual review required before approving new model version")
        logger.warning("="*80)

        # DO NOT save to database - exit early
        return

    # Model performance acceptable - save to DB
    logger.info(f"Model performance acceptable - saving to database")

    # write to DB
    run_df = pd.DataFrame([{
        "model_version": model_version,
        "threshold_type": threshold_type,
        "threshold_value": threshold
    }])
    write_sql(run_df, versions_table_name)

    # read run_id that was just inserted
    run_id = read_sql("SELECT MAX(run_id) as run_id FROM forecasting.model_runs")["run_id"].iloc[0]

    metrics = [
        {"run_id": run_id, "model_type": "classifier", "metric_name": "precision", "metric_value": precision},
        {"run_id": run_id, "model_type": "classifier", "metric_name": "recall", "metric_value": recall},
        {"run_id": run_id, "model_type": "classifier", "metric_name": "f1", "metric_value": f1},
        {"run_id": run_id, "model_type": "classifier", "metric_name": "accuracy", "metric_value": accuracy},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "mae", "metric_value": mae},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "accurate", "metric_value": business_metrics["accurate"]},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "accurate_share", "metric_value": business_metrics["accurate_share"]},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "waste", "metric_value": business_metrics["waste"]},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "waste_share", "metric_value": business_metrics["waste_share"]},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "lost_sale", "metric_value": business_metrics["lost_sale"]},
        {"run_id": run_id, "model_type": "regressor", "metric_name": "lost_sale_share", "metric_value": business_metrics["lost_sale_share"]},
    ]
    write_sql(pd.DataFrame(metrics), "model_metrics")

    # features importance
    cls_importance = classifier.get_feature_importance()
    reg_importance = regressor.get_feature_importance()
    feature_names = classifier.feature_names_

    feature_df = pd.DataFrame({
        "feature_name": feature_names,
        "classifier_importance" : cls_importance,
        "regressor_importance": reg_importance})
    feature_df["run_id"] = run_id

    write_sql(feature_df, "feature_importance")

    business_metrics_by_item["run_id"] = run_id
    write_sql(business_metrics_by_item, "model_metrics_by_item")