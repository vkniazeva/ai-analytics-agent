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


## Staging

| Concept       | Purpose                                                 | Who loads it            |
|---------------|---------------------------------------------------------|-------------------------|
| Source (raw)  | Operational data from files/APIs — changes frequently   | `ingestion/load_raw.py` |
| Model         | SQL transformation of data already in the DB            | dbt (`dbt run`)         |
| Seed          | Small lookup/reference data that ships with the project | dbt (`dbt seed`)        |

Used seeds:

`seeds/cities_mapping.csv` - Maps IATA airport codes to anonymized city IDs

The data is taken by dbt from the raw schema tables, and then they are processed based on the 
SQL models definition from models/staging. All models refer to the sources.yml file to their corresponding 
tables, and then transformed by performing the following steps:
1. Columns renaming
2. Transforming: casting to the target data type, mapping (anonymizing) needed values, setting
null values where applicable to the default values.
3. Cleaning: checking the allowed values, removing null records 

All staging models are created as views in PostreSQL under the public schema.

### Testing

1. Schema consistency
   - naming
   - types
   - casts
2. Null handling
3. Grain validation
4. Basic domain validation
   - accepted_values
   - amount >= 0
   - dates valid
5. Referential sanity (relationships)

Not all fields are covered with the tests, but only the business critical ones.

#### Staging 

On the staging step the following tests are implemented:
- not null values
- allowed values for the specific enum columns
- grain tests are removed from this level!
- custom sql tests are added


## Full workflow

```bash
docker-compose up -d              # 1. Start PostgreSQL
python -m ingestion.load_raw      # 2. Load raw files into DB
cd analytics_dbt
dbt seed --profiles-dir .         # 3. Load seed lookup tables
dbt run --profiles-dir .          # 4. Run transformations (staging → dwh → marts)
# dbt test --profiles-dir .         # 5. Validate data quality
```

## Legacy Python code

The original Python ETL files (`etl/staging.py`, `etl/dwh.py`, `etl/presentation.py`) are kept in the repo for reference during migration. They will be moved to `etl_legacy/` once dbt models are validated.

## Useful links
Modelling with dbt:
- [Building a Kimball dimensional model with dbt](https://docs.getdbt.com/blog/kimball-dimensional-model?version=1.13)
- [Data Vault 2.0 with dbt Cloud](https://docs.getdbt.com/blog/data-vault-with-dbt-cloud?version=1.13)
- [Medallion Architecture with dbt](https://tsaiprabhanj.medium.com/medallion-architecture-with-dbt-a40050743be3)
- [Database Normalization vs. Denormalization](https://medium.com/analytics-vidhya/database-normalization-vs-denormalization-a42d211dd891)