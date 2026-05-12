# dbt Project: Analytics

## Overview

This dbt project replaces the Python-based ETL pipeline (Pandas) with SQL-based transformations inside PostgreSQL (ELT pattern).

**Before (ETL):** CSV/Excel -> staging.py (Pandas) -> Parquet -> dwh.py -> Parquet -> db.py -> PostgreSQL

**After (ELT):** CSV/Excel -> Python loader (raw tables) -> PostgreSQL -> dbt (all transformations in SQL)

## Setup

### Prerequisites

- PostgreSQL running via Docker (`docker-compose up`)
- Python virtualenv with `dbt-postgres` installed

### Installation

```bash
pip install dbt-postgres
```

### Connection Profile

The profile lives in `analytics/profiles.yml` and uses env vars from `.env`.

**Key settings explained:**

| Setting    | Value    | Why                                                                   |
|------------|----------|-----------------------------------------------------------------------|
| `target`   | `dev`    | Default environment selector. Can add `prod`, `ci` later              |
| `schema`   | `public` | Default namespace for tables. Can split into `staging`, `marts` later |
| `threads`  | `4`      | Models built in parallel. Match to CPU cores / DB capacity            |

**`target` vs `outputs`:** `outputs` lists all possible environments; `target` picks which one to use by default. Override with `dbt run --target prod`.

### Running dbt

dbt does not read `.env` files automatically. Export env vars first:

```bash
export $(cat ../.env | grep -v '#' | xargs)
```

Then run commands from the `analytics/` directory:

```bash
dbt debug --profiles-dir .     # verify connection
dbt run --profiles-dir .       # build models
dbt test --profiles-dir .      # run tests
dbt seed --profiles-dir .      # load seed CSV files
```

## Project Structure

```
analytics/
  dbt_project.yml    -- project configuration
  profiles.yml       -- database connection (uses env vars, safe to commit)
  models/            -- SQL transformations
    staging/         -- clean & rename (replaces staging.py)
    dwh/             -- star schema: dims & facts (replaces dwh.py)
    marts/           -- aggregated analytical views (replaces presentation.py)
  seeds/             -- small CSV lookup tables (e.g. cities_mapping)
  tests/             -- custom data tests
  macros/            -- reusable SQL snippets
```

## How dbt replaces Python transformations

| Python (staging.py)                         | dbt equivalent                                     |
|---------------------------------------------|----------------------------------------------------|
| `rename_cols()`                             | `SELECT "Old Name" AS new_name`                    |
| `format_cols()` (type casting)              | `::int`, `::numeric`, `::date` in SQL              |
| `process_flight_data()` (prefix + city map) | SQL string ops + JOIN to seed table                |
| `drop_duplicates()`                         | `SELECT DISTINCT`                                  |
| `drop_invalid_nan(df, required_cols)`       | `WHERE col IS NOT NULL` for each required column   |
| `filter_negatives()`                        | `WHERE col >= 0`                                   |
| Date range filter                           | `WHERE date BETWEEN '2026-01-01' AND '2026-03-31'` |
| Save to parquet                             | dbt materializes as table/view automatically       |

## Seeds

### What are seeds?

Seeds are a **dbt concept** — small, static CSV files that dbt loads into the database as tables. 

**Why seeds exist separately from raw data:**

| Concept       | Purpose                                                 | Who loads it            |
|---------------|---------------------------------------------------------|-------------------------|
| Source (raw)  | Operational data from files/APIs — changes frequently   | `ingestion/load_raw.py` |
| Model         | SQL transformation of data already in the DB            | dbt (`dbt run`)         |
| Seed          | Small lookup/reference data that ships with the project | dbt (`dbt seed`)        |

**Why use seeds instead of loading via the ingestion script?**

1. **Version controlled with transformations** — the lookup CSV and the SQL that JOINs to it live together in `analytics/`
2. **No external dependency** — `dbt seed` handles it; no separate Python step needed
3. **Self-documenting** — anyone reading the project sees the reference data inline
4. **Easy to update** — edit the CSV, run `dbt seed` again

### Current seeds

| File                        | Purpose                                          | Used by                                      |
|-----------------------------|--------------------------------------------------|----------------------------------------------|
| `seeds/cities_mapping.csv`  | Maps IATA airport codes to anonymized city IDs   | Staging models (origin/destination mapping)  |

### Usage

```bash
dbt seed --profiles-dir .     # loads all CSVs in seeds/ as tables
```

Reference in SQL models:
```sql
LEFT JOIN {{ ref('cities_mapping') }} cm ON raw_table.origin = cm.iata_code
```

## Ingestion (raw data loading)

The `ingestion/` folder contains the script that loads source files (CSV/Excel) into PostgreSQL's `raw` schema. This is the "E+L" in ELT — dbt handles the "T".

```bash
python -m ingestion.load_raw    # or: python ingestion/load_raw.py
```

**What it does:**
- Reads all files from `data/raw/`
- Concatenates monthly files (e.g. 3 months of sales → one `raw.sales` table)
- Drops prohibited columns (`Staff ID`, `Staff Name` from sales — PII)
- Loads into PostgreSQL `raw` schema with no transformations
- Idempotent: safe to re-run (drops and recreates tables)

**Raw tables created:**

| Table                 | Source files                                      |
|-----------------------|---------------------------------------------------|
| `raw.pax`             | `pax_jan_2026.csv` … `pax_mar_2026.csv`           |
| `raw.sales`           | `sales_jan_2026.xlsx` … `sales_mar_2026.xlsx`     |
| `raw.payments`        | `payment_jan_2026.xlsx` … `payment_mar_2026.xlsx` |
| `raw.wastage`         | `wastage_jan_2026.xlsx` … `wastage_mar_2026.xlsx` |
| `raw.schedule`        | `schedule.csv`                                    |
| `raw.product_catalog` | `product_catalog_all.xlsx`                        |
| `raw.bank`            | `bank.csv`                                        | 

## Full workflow

```bash
docker-compose up -d              # 1. Start PostgreSQL
python -m ingestion.load_raw      # 2. Load raw files into DB
cd analytics
#TBD
# dbt seed --profiles-dir .         # 3. Load seed lookup tables
# dbt run --profiles-dir .          # 4. Run transformations (staging → dwh → marts)
# dbt test --profiles-dir .         # 5. Validate data quality
```

## Legacy Python code

The original Python ETL files (`etl/staging.py`, `etl/dwh.py`, `etl/presentation.py`) are kept in the repo for reference during migration. They will be moved to `etl_legacy/` once dbt models are validated.
