.PHONY: help install start init-db load dbt test metadata-sync run forecast pipeline

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies (activate your venv first)
	pip install -r requirements.txt

start: ## Start PostgreSQL
	docker-compose up -d postgres
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3

init-db: ## Initialize database tables from scripts/create_tables.sql
	@echo "Creating database tables..."
	@docker-compose exec -T postgres bash -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB' < scripts/create_tables.sql
	@echo "Database tables created successfully"

load: ## Load raw data into PostgreSQL
	python -m ingestion.load_raw

dbt: ## Run dbt end-to-end (deps + seed + run + test)
	cd analytics_dbt && dbt deps --profiles-dir . && dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .

test: ## Run dbt tests only
	cd analytics_dbt && dbt test --profiles-dir .

metadata-sync: ## Sync metadata from dbt to database
	python -m metadata.metadata_sync

run: start init-db load dbt metadata-sync ## Full pipeline: start db, load data, run dbt, sync metadata

forecast: ## Run forecasting (uses data_source from config.yaml: mock or database)
	~/.virtualenvs/ai-analytics-agent/bin/python -m forecasting.run

pipeline: start init-db load dbt metadata-sync ## Complete pipeline: analytics + forecasting
	@echo "Ensure config.yaml has data_source: database for production pipeline"
	~/.virtualenvs/ai-analytics-agent/bin/python -m forecasting.run

api: ## Run forecasting API server
	~/.virtualenvs/ai-analytics-agent/bin/uvicorn forecasting.api.app:app --reload