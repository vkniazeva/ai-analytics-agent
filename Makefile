.PHONY: help install start load dbt test metadata-sync run forecast pipeline

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies (activate your venv first)
	pip install -r requirements.txt

start: ## Start PostgreSQL
	docker-compose up -d postgres

load: ## Load raw data into PostgreSQL
	python -m ingestion.load_raw

dbt: ## Run dbt end-to-end (deps + seed + run + test)
	cd analytics_dbt && dbt deps --profiles-dir . && dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .

test: ## Run dbt tests only
	cd analytics_dbt && dbt test --profiles-dir .

metadata-sync: ## Sync metadata from dbt to database
	python -m metadata.metadata_sync

run: start load dbt metadata-sync ## Full pipeline: start db, load data, run dbt, sync metadata

forecast: ## Run forecasting (uses data_source from config.yaml: mock or database)
	python -m forecasting.run

pipeline: start load dbt metadata-sync ## Complete pipeline: analytics + forecasting
	@echo "Ensure config.yaml has data_source: database for production pipeline"
	python -m forecasting.run
