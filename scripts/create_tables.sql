-- forecasting tables

CREATE SCHEMA IF NOT EXISTS forecasting;

CREATE TABLE IF NOT EXISTS forecasting.model_runs (
    run_id SERIAL PRIMARY KEY,
    model_version VARCHAR,
    threshold_type VARCHAR,
    threshold_value FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS forecasting.model_metrics (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES forecasting.model_runs(run_id),
    model_type VARCHAR,
    metric_name VARCHAR,
    metric_value FLOAT
);

CREATE TABLE IF NOT EXISTS forecasting.model_metrics_by_item (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES forecasting.model_runs(run_id),
    item_id VARCHAR,
    accurate INTEGER,
    waste INTEGER,
    lost_sale INTEGER
);

CREATE TABLE IF NOT EXISTS forecasting.feature_importance (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES forecasting.model_runs(run_id),
    feature_name VARCHAR,
    classifier_importance FLOAT,
    regressor_importance FLOAT
);