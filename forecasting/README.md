# Fresh Food Sales Forecasting

Production-ready ML pipeline for predicting fresh food demand on airline flights.

---

## 1. Business Context

Fresh food sales are highly sensitive for airlines due to strict expiration constraints. The balance between **lost sales** (stockouts) and **waste** (unused inventory) is critical to profitability.

**Key Challenge:**  
Unlike standard products, fresh items cannot be carried over to the next flight, making accurate demand forecasting essential.

**Business Requirement:**  
The model must provide not only high accuracy but also **configurable business objectives**:
- **Minimize waste** - Conservative predictions to reduce spoilage
- **Minimize lost sales** - Aggressive predictions to maximize revenue

This is achieved through adjustable classification thresholds (`low_wastage` vs `low_missed_sales`) that shift the precision-recall tradeoff based on business priorities.

---

## 2. Pipeline Logic

The forecasting pipeline follows a standard ML workflow with production-grade safeguards:

```
1. Data Ingestion       → Load from PostgreSQL mart or mock parquet
2. Data Cleanup         → Remove errors, duplicates, missing values
3. Feature Engineering  → Historical averages, passenger bins, routes
4. Data Validation      → Schema checks, date ranges, distributions
5. Model Training       → Two-stage CatBoost (classifier + regressor)
6. Model Evaluation     → Technical + business metrics, degradation detection
7. Model Registry       → Save models + metrics to database (if no degradation)
```

**Key Design Principles:**
- **Config-driven** - All parameters in `config.yaml`, no hardcoded values
- **Mock/prod parity** - Same code runs with test data or live database
- **Fail-safe degradation handling** - If model performance drops >10%, pipeline logs critically but does NOT deploy the new model, keeping the old model active

---

## 3. Module Overview

### `data_preparation/`

#### `data_ingestion/`
**What:** Abstract data source interface  
**How:** `MockSource` (parquet) or `DBSource` (PostgreSQL)  
**Why:** Enables testing without database; single `fetch()` interface works for any source

#### `data_cleanup/`
**What:** Data quality filtering  
**How:** Remove `potential_error` flags, deduplicate by `flight_key + item_id`, handle NaNs  
**Why:** Prevents garbage-in-garbage-out; eliminates quality issues identified in EDA

#### `feature_engineering/`
**What:** Create predictive features  
**How:**
- **Passenger bins** - `<100, 100-150, 150-180, 180+` (non-linear sales relationship)
- **Routes** - `origin_destination` strings for aggregation
- **Historical averages** - 4-level hierarchical fallback:
  1. `item × route × day_period` (most specific)
  2. `item × route × is_night`
  3. `item × day_period`
  4. `item` (least specific, always available)

**Why:** Historical patterns are the strongest predictor (from modeling analysis); hierarchical fallback ensures all items get a baseline prediction even with sparse data

#### `data_validation/`
**What:** Schema and quality checks  
**How:** Validate required columns, date ranges, target distribution, no unexpected nulls  
**Why:** Fail fast if upstream data changes; catch silent data drift

---

### `model/`

#### `model_building/train.py`
**What:** Two-stage CatBoost model  
**How:**
1. **Classifier** - Predict zero vs non-zero sales (handles 63.5% zero-inflation)
2. **Regressor** - Predict quantity (only for non-zero predictions)

**Why:** Zero-inflation means standard regression performs poorly; two-stage approach improves MAE by 5%

#### `model_evaluation/evaluate.py`
**What:** Metrics calculation and degradation detection  
**How:**
- **Technical metrics** - Accuracy, precision, recall, F1, MAE
- **Business metrics** - Accurate %, waste %, lost sale %
- **Degradation check** - Compare accuracy to last deployed model
  - If drop >10%: Log CRITICAL, show metrics, DO NOT save model, return early
  - If acceptable: Save model + metrics to database

**Why:** Production models must be monitored; degraded models should not auto-deploy

#### `model_registry/handle_model.py`
**What:** Model persistence and loading  
**How:** Save/load CatBoost models as `.cbm` files with version tracking  
**Why:** Enables rollback, versioning, and consistent predictions across API instances

---

### `api/`

**What:** FastAPI service for real-time predictions

**Business Context:**  
Predictions are made per **flight segment** (e.g., `JFK_LHR` or `LHR_CDG`). The entire route must be requested separately by segments since fresh food is loaded at each origin.

#### Endpoints

All endpoints share the same request body (model features):
```json
{
  "route": "JFK_LHR",
  "expected_pax": 180,
  "day_period": "Morning"
}
```

**1. `POST /predict/{threshold_type}`**  
Returns expected sales quantity for **all active fresh products** on this flight segment.

**Business use case:** Primary forecasting endpoint. Operations team uses this to determine initial stocking quantities across all items.

**2. `POST /predict/{threshold_type}/item/{item_id}`**  
Returns detailed prediction for a **single item**, including:
- Predicted quantity
- Historical average sales
- Estimated accuracy (from latest model evaluation)

**Business use case:** When the business doubts the predicted numbers (e.g., prediction seems too high/low), they call this endpoint to see the historical baseline and model confidence. Helps justify or override automated forecasts.

**3. `POST /predict/{threshold_type}/category`**  
Returns aggregated predictions by **product category** (e.g., Sandwiches, Salads, Snacks).

**Business use case:** Enables category-level planning. Important for offering choice including alternative products on board. If one sandwich variant is low stock, business can substitute with another from the same category.

**Threshold Types:**
- `low_missed_sales` (threshold=0.4) - Maximize revenue, accept higher waste
- `low_wastage` (threshold=0.7) - Minimize spoilage, accept lost sales

---

## 4. Key Configuration

All settings in `forecasting/configs/config.yaml`. Most critical parameters:

### Data Source
```yaml
data_preparation:
  data_ingestion:
    data_source: database  # mock | database
```

### Business Thresholds
```yaml
model:
  catboost:
    low_missed_sales: 0.4   # Aggressive: prioritize sales
    low_wastage: 0.7        # Conservative: prioritize waste reduction
```

### Model Safeguards
```yaml
model:
  degradation_threshold: 0.1  # Max acceptable accuracy drop (10%)
  test_split_by_weeks: 2      # Last 2 weeks for validation
```

---

## 5. Usage

### Run Full Pipeline
```bash
make pipeline
```
Executes: start PostgreSQL → init database → load raw data → run dbt → sync metadata → train forecasting model

### Run Forecasting Only
```bash
make forecast
```
Runs data preparation + model training with current config

### Start API Server
```bash
make api
```
Launches FastAPI at `http://localhost:8000` (auto-reload enabled)

**API Documentation:** `http://localhost:8000/docs`

---

## 6. Production Safeguards

### Model Degradation Handling

If new model accuracy drops >10% compared to previous deployed model:

**Actions:**
1. Log CRITICAL alert with degradation details
2. Log all metrics (classifier, regressor, business) for review
3. DO NOT save new model to database
4. Keep old model active
5. Pipeline completes successfully (no exception raised)

**Why:** Scheduled pipelines should not fail on degradation, but degraded models must not auto-deploy. Manual review is required before approving the new version.

### Data Validation

Pipeline fails immediately if:
- Required columns missing
- Date range outside expected bounds
- Target distribution anomalous
- Unexpected nulls in key fields

**Why:** Fail fast on data quality issues; prevent silent failures downstream

---

## 7. Database Schema

### Model Registry Tables

**`forecasting.model_runs`**
```sql
run_id (PK, auto-increment)
model_version
threshold_type
threshold_value
created_at
```

**`forecasting.model_metrics`**
```sql
run_id (FK)
model_type (classifier | regressor)
metric_name (accuracy, precision, recall, f1, mae, accurate_share, waste_share, lost_sale_share)
metric_value
```

**`forecasting.model_metrics_by_item`**
```sql
run_id (FK)
item_id
accurate (count)
waste (count)
lost_sale (count)
```

**`forecasting.feature_importance`**
```sql
run_id (FK)
feature_name
classifier_importance
regressor_importance
```

**`forecasting.lookup_hist_avg`**
```sql
item_id
route
day_period
is_night
hist_avg
hist_level_used (1-4)
```

---

## 8. Model Performance

**Target Distribution:** 63.5% zero-inflation (many flights sell nothing for certain items)

**Business Metrics (Test Set):**
- Accurate predictions: ~70%
- Waste cases: ~25%
- Lost sale cases: ~5%

**Technical Metrics:**
- MAE: 0.45 (5% improvement over baseline)
- Classifier accuracy: 85%
- Regressor MAE (on non-zero cases): 0.38

**Key Insight:** Flight-level granularity significantly outperforms line-level aggregation
