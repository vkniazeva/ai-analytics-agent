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

Small lookup data (like `cities_mapping.json`) becomes a CSV in `seeds/`. Run `dbt seed` to load it as a table in the database. Reference in models with `{{ ref('cities_mapping') }}`.

## Legacy Python code

The original Python ETL files (`etl/staging.py`, `etl/dwh.py`, `etl/presentation.py`) are kept in the repo for reference during migration. They will be moved to `etl_legacy/` once dbt models are validated.
