import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor
from fastapi import FastAPI, HTTPException, Request
from typing import List
from contextlib import asynccontextmanager

from forecasting.api.schemas import (
    ThresholdType, PredictRequest, PredictItemsResponse,
    PredictItemResponse, PredictCategoriesResponse
)
from forecasting.model.model_registry.handle_model import load_model
from forecasting.utils.config_handler import return_config
from forecasting.utils.database import read_sql


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.classifier, app.state.regressor = load_model()
    yield


app = FastAPI(lifespan=lifespan)
config = return_config()
data_source = config["data_preparation"]["data_ingestion"]["data_source"]


# --- helpers ---

def _map_bins(pax: int) -> str:
    bins = config["data_preparation"]["feature_engineering"]["pax_bins"]
    match pax:
        case v if v < bins[1]: return "<100"
        case v if v < bins[2]: return "100 - 150"
        case v if v < bins[3]: return "150 - 180"
        case _: return "180 +"


def _load_fresh_products() -> pd.DataFrame:
    query = "SELECT item_id, category FROM mart.dim_products WHERE item_type = 'Fresh Product' AND category != 'BOL Products' AND status = 'Active'"
    return read_sql(query, "dim_products") if data_source == "mock" else read_sql(query)


def _load_hist_avg() -> pd.DataFrame:
    query = "SELECT * FROM forecasting.lookup_hist_avg"
    return read_sql(query, "lookup_hist_avg") if data_source == "mock" else read_sql(query)


def _prepare_data(route: str, day_period: str, expected_pax: int) -> pd.DataFrame:
    pax_bin = _map_bins(expected_pax)
    hist_avg = _load_hist_avg()
    products = _load_fresh_products()

    items_df = pd.DataFrame({
        "item_id": products["item_id"],
        "route": route,
        "pax_bin": pax_bin,
        "day_period": day_period
    })
    items_df = items_df.merge(
        hist_avg[["item_id", "route", "day_period", "hist_avg"]],
        on=["item_id", "route", "day_period"],
        how="left"
    )
    return items_df


def _process_classification(df: pd.DataFrame, classifier: CatBoostClassifier,
                             threshold_type: str, features: list) -> pd.DataFrame:
    threshold = config["model"]["catboost"][threshold_type]
    df = df.copy()
    df["cls_predict_proba"] = classifier.predict_proba(df[features])[:, 1]
    df["cls_predict"] = (df["cls_predict_proba"] >= threshold).astype(int)
    return df


def _process_regression(items_df: pd.DataFrame, regressor: CatBoostRegressor,
                         features: list) -> pd.DataFrame:
    items_df_reg = items_df[items_df["cls_predict"] == 1].copy()
    items_df_neg = items_df[items_df["cls_predict"] == 0].copy()
    items_df_neg["predicted"] = 0

    if len(items_df_reg) > 0:
        items_df_reg["predicted"] = regressor.predict(
            items_df_reg[features]
        ).round(0).astype(int)

    return pd.concat([items_df_neg, items_df_reg], ignore_index=True)


def _get_estimated_accuracy(item_id: str) -> float:
    query = f"""
        SELECT mi.accurate::float / NULLIF(mi.accurate + mi.waste + mi.lost_sale, 0) as estimated_accuracy
        FROM forecasting.model_metrics_by_item mi
        JOIN forecasting.model_runs mr ON mi.run_id = mr.run_id
        WHERE mi.item_id = '{item_id}'
        ORDER BY mr.run_id DESC
        LIMIT 1
    """
    result = read_sql(query, "model_metrics_by_item")
    return float(result["estimated_accuracy"].iloc[0]) if len(result) > 0 else None


# --- endpoints ---

FEATURES = ["item_id", "route", "pax_bin", "day_period", "hist_avg"]


@app.post("/predict/{threshold_type}", response_model=List[PredictItemsResponse])
def predict_all_items(threshold_type: ThresholdType, request: PredictRequest, req: Request):
    items_df = _prepare_data(request.route, request.day_period, request.expected_pax)
    classifier = req.app.state.classifier
    regressor = req.app.state.regressor

    items_df = _process_classification(items_df, classifier, threshold_type, FEATURES)
    all_items = _process_regression(items_df, regressor, FEATURES)

    return all_items[["item_id", "predicted"]].rename(
        columns={"predicted": "predicted_quantity"}
    ).to_dict(orient="records")


@app.post("/predict/{threshold_type}/item/{item_id}", response_model=PredictItemResponse)
def predict_item(threshold_type: ThresholdType, item_id: str,
                 request: PredictRequest, req: Request):
    items_df = _prepare_data(request.route, request.day_period, request.expected_pax)

    single_item_df = items_df[items_df["item_id"] == item_id].copy()
    if len(single_item_df) == 0:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found")

    classifier = req.app.state.classifier
    regressor = req.app.state.regressor

    single_item_df = _process_classification(single_item_df, classifier, threshold_type, FEATURES)
    result_df = _process_regression(single_item_df, regressor, FEATURES)

    row = result_df.iloc[0]
    hist_avg_value = float(single_item_df["hist_avg"].iloc[0]) \
        if not single_item_df["hist_avg"].isna().all() else 0.0

    return PredictItemResponse(
        item_id=item_id,
        threshold_type=threshold_type,
        threshold_value=config["model"]["catboost"][threshold_type],
        predicted_quantity=int(row["predicted"]),
        historical_average=hist_avg_value,
        estimated_accuracy=_get_estimated_accuracy(item_id)
    )


@app.post("/predict/{threshold_type}/category", response_model=List[PredictCategoriesResponse])
def predict_by_category(threshold_type: ThresholdType, request: PredictRequest, req: Request):
    items_df = _prepare_data(request.route, request.day_period, request.expected_pax)
    products = _load_fresh_products()

    # add category
    items_df = items_df.merge(products[["item_id", "category"]], on="item_id", how="left")

    classifier = req.app.state.classifier
    regressor = req.app.state.regressor

    items_df = _process_classification(items_df, classifier, threshold_type, FEATURES)
    all_items = _process_regression(items_df, regressor, FEATURES)

    # sum by categories
    by_category = all_items.groupby("category")["predicted"].sum().reset_index()
    by_category = by_category.rename(columns={
        "category": "category_name",
        "predicted": "predicted_quantity"
    })

    return by_category.to_dict(orient="records")