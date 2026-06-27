# Fresh Food Sales Forecasting Module

Production-ready forecasting pipeline for predicting fresh food demand on flights.

## Table of Contents
- [Module Organization](#module-organization)
- [Architecture](#architecture)
- [Data Preparation Pipeline](#data-preparation-pipeline)
- [Configuration](#configuration)
- [Usage](#usage)

---

## Module Organization

```
forecasting/
├── analysis/                      # EDA notebooks & modeling experiments
├── configs/                       # YAML configuration
├── data_preparation/              
│   ├── data_ingestion/            # Abstract data sources (mock/database)
│   ├── data_cleanup/              # Error filtering, deduplication
│   ├── data_validation/           # Schema & quality checks
│   └── feature_engineering/       # Historical averages, binning
├── utils/                         # Config & database helpers
├── interim_files/                 # Mock data for development
└── run.py                         # Pipeline orchestrator
```

---

## Architecture

### Design Principles

1. **Separation of Concerns** - Each module handles a single responsibility
2. **Abstract Base Classes** - Data sources implement common interface for easy switching
3. **Configuration-Driven** - All parameters centralized in YAML for reproducibility
4. **Mock/Production Parity** - Same code runs with test data or production database

### Pipeline Flow

TBD


## Data Preparation Pipeline

### 1. Data Ingestion

**Purpose:** Load data from different sources without changing downstream code

**Why Abstract Base Class:**
- Single interface (`fetch()`) works for any source
- Easy to add new sources (API, file, etc.)
- Enables testing without database

**Sources:**
- **MockSource** - Parquet file for development/testing
- **DBSource** - PostgreSQL mart table for production

### 2. Data Cleanup

**Purpose:** Ensure data quality before modeling

**Steps:**
- Filter records with `potential_error` flags
- Remove duplicates at grain level (flight_key + item_id)
- Handle missing values
- Standardize data types

**Why:** Prevents garbage-in-garbage-out; eliminates data quality flags identified in EDA

### 3. Data Validation

**Purpose:** Catch schema changes and data drift early

**Checks:**
- Required columns present
- Date range within expected bounds
- Target variable distribution reasonable
- No unexpected nulls in key fields

**Why:** Fail fast if upstream data changes; avoid silent failures in production

### 4. Feature Engineering

**Purpose:** Create predictive features based on EDA findings

**Features:**
- **Passenger bins** - Non-linear relationship with sales (thresholds: 100, 150, 180)
- **Historical averages** - 3-level hierarchical fallback (item×route×period → item×route → item)
- **Route strings** - Aggregated origin-destination sequences

**Why:** Historical patterns are strongest predictor (from modeling analysis); binning captures non-linear effects


## Configuration

### Structure

All parameters centralized in `configs/config.yaml`:

```yaml
data_preparation:
  data_ingestion:
    data_source: mock          # mock | database
  
  feature_engineering:
    pax_bins: [100, 150, 180]  # Passenger thresholds - only inner boundaries
    min_samples: 5             # Minimum historical samples
  
  data_validation:
    required_columns: [...]    # Schema validation
    start_date: "2025-11-01"   # Expected date range
    end_date: "2026-02-28"
```

**Why YAML:**
- Human-readable and version-controlled
- Easy to switch between dev/prod settings
- No code changes needed for parameter tuning

**Environment-Specific:**
- Development: `data_source: mock`
- Production: `data_source: database` (credentials from env vars)

---

## Usage

**`make forecast`:**
```bash
make forecast
```
1. Loads config from YAML
2. Initializes data source (mock or database)
3. Runs data preparation pipeline
4. (TODO) Generates predictions

**`make pipeline`:**
```bash
make pipeline
```
1. Starts PostgreSQL
2. Loads raw data
3. Runs dbt transformations
4. Syncs metadata
5. Executes forecasting


## Development

### Mock Data

**Purpose:** Develop and test without database dependency

**Location:** `interim_files/raw_data_df.parquet`

**Why:** Faster iteration, no database setup needed, consistent test data

### Testing Strategy

1. **Unit Tests** - Each module independently with mock data
2. **Integration Tests** - Full pipeline with test database
3. **Validation Tests** - Predictions within expected ranges
4. **Performance Tests** - Track execution time

---

## Key Findings

See [`analysis/README.md`](./analysis/README.md) for complete EDA and modeling documentation.

**Highlights:**
- Target: 63.5% zero-inflation → two-stage CatBoost (classifier + regressor)
- Best MAE: 0.45 (5% improvement over baseline)
- Key features: historical averages, item_id, route, passenger bins
- **Important:** Flight-level granularity significantly outperforms line-level aggregation

---

## Next Steps

- [ ] Complete data cleanup & validation modules
- [ ] Implement feature engineering pipeline
- [ ] Add CatBoost model inference
- [ ] Create prediction output format
- [ ] Add comprehensive testing
- [ ] Implement logging & monitoring
- [ ] Create CI/CD pipeline
