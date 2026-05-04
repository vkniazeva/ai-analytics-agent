# AI Analytics Agent

An AI-powered analytics system that enables natural language exploration of structured business data using a layered data 
architecture, semantic analytics API, and LLM-driven insights generation.

---

## Table Of Content

1. [Business Context](#business-context)
2. [Features](#features)
3. [Architecture](#architecture)
4. [Data Handling](#data-handling)
5. [Data Warehouse (Star Schema)](#data-warehouse-star-schema)
6. [Data Model (ER Diagram)](#data-model-er-diagram)
7. [Setup](#setup)
8. [Changelog and State](#changelog-and-state)
9. [Other](#other)

---

## Business Context

Modern analytics systems often suffer from:

* fragmented data sources
* inconsistent metric definitions
* strong dependency on engineers for insights

This project simulates an airline retail analytics environment and demonstrates how an AI layer can simplify data exploration.

---

## Features

* Layered data architecture (raw → processed → DWH → marts)
* ETL pipeline for structured data preparation
* Star schema data warehouse
* PostgreSQL serving layer
* Superset BI dashboards
* (Planned) Analytics API
* (Planned) LLM-powered analytics agent

---

## Architecture

```mermaid
flowchart LR

    subgraph Data_Layer
        A[Raw Data CSV Excel]
    end

    subgraph Processing_Layer
        B[ETL Pandas Cleaning Standardization]
        C[Processed Layer Parquet]
        D[DWH Layer Star Schema]
        E[Marts Layer]
    end

    subgraph Serving_Layer
        F[PostgreSQL]
        G[Superset]
        H[Analytics API Future]
    end

    subgraph AI_Layer
        I[LLM Agent Future]
    end

    A --> B --> C --> D --> E --> F --> G
    F --> H --> I
```

---

## Data Handling

### Data Processing Flow

| Step | Layer            | Description                                   |
| ---- | ---------------- | --------------------------------------------- |
| 1    | `data/raw`       | Raw CSV / Excel files                         |
| 2    | `data/processed` | Cleaned & standardized data (temporary layer) |
| 3    | `data/dwh`       | Star schema (dims + facts)                    |
| 4    | `data/marts`     | Aggregated analytical tables                  |
| 5    | PostgreSQL       | Serving layer                                 |

---

### ⚠️ Important Note on Processing Layer

The **processed layer (Parquet + Pandas)** is currently a **temporary implementation**.

Planned evolution:

* Replace Pandas transformations with **SQL-based transformations inside the data warehouse**
* Move toward:

  * ELT instead of ETL
  * dbt / SQL models (or similar approach)
  * database-native joins and aggregations

> Pandas is used here as a prototyping tool, not as a final data processing engine.

---

### Data Sources

Synthetic airline retail data:

* Flight sales transactions
* Passenger data
* Payment transactions
* Inventory / wastage data
* Flight schedule
* Product catalog
* Bank metadata

---

### Data Processing

The processed layer ensures:

* standardized column names
* consistent data types
* anonymization
* cross-source enrichment
* basic data quality checks:

  * drop duplicates
  * drop invalid NULLs
  * filter negative values

---

## Data Warehouse (Star Schema)

### Dimensions

* `dim_date`
* `dim_flight`
* `dim_product`
* `dim_session`
* `dim_card`
* `dim_load`

### Facts

* `fact_sales`
* `fact_payment`
* `fact_pax`
* `fact_wastage`

Dimension-Fact Relationships:
```mermaid
flowchart LR

    dim_flight --> fact_pax
    dim_flight --> fact_payment
    dim_flight --> fact_sales
    dim_flight --> fact_wastage

    dim_session --> fact_payment
    dim_session --> fact_sales

    dim_product --> fact_sales
    dim_product --> fact_wastage

    dim_card --> fact_payment

    dim_load --> fact_wastage

    dim_date --> fact_sales
```
---

### Fact Table Grains

| Table        | Grain                  |
| ------------ | ---------------------- |
| fact_sales   | item-level transaction |
| fact_payment | payment event          |
| fact_pax     | flight + class         |
| fact_wastage | product per flight     |

---

## Data Model (ER Diagram)

![Data Model](docs/diagrams/data_model.svg)

---

## Data Marts

![Data Marts](docs/diagrams/data_marts.svg)



### Available Marts

* `mart_sales_performance`
* `mart_product_sales`
* `mart_flight_sales`

---

## Key Design Decisions

* Star schema for analytical clarity
* Surrogate keys for all entities
* Degenerate dimensions (e.g. `slip_id`)
* Columnar storage (Parquet)
* PostgreSQL as serving layer
* Superset as BI layer

---

## Setup

### Prerequisites

* Python 3.13+
* Docker & Docker Compose

---

### Installation & Run

```bash
# Start database
docker compose up -d postgres

# Run full pipeline
python app.py --etl

# Start Superset
docker compose up -d superset
```

Open:

```
http://localhost:8088
```

Login:

```
admin / admin
```

---

## Changelog and State

### Completed

* ETL pipeline
* Data staging layer
* Data warehouse (star schema)
* PostgreSQL loading (COPY)
* PK/FK constraints and indexes
* Data marts
* Superset dashboards

---

### In Progress

* Data model stabilization
* Migration from Pandas → SQL transformations

---

### Next Steps

* Analytics API (FastAPI)
* Semantic layer (metrics definitions)
* LLM agent integration

---

## Other

### Data Samples

| Layer     | Location                  |
| --------- | ------------------------- |
| Processed | `data/processed/samples/` |
| DWH       | `data/dwh/samples/`       |
| Marts     | `data/marts/samples/`     |

---

### Notes

* Data is anonymized
* Mapping configs are external
* Raw data is not version-controlled
* Processed layer is temporary and will be replaced with SQL-based transformations

### Regenerating Diagrams

PlantUML sources are in `docs/diagrams/`. To regenerate SVGs:

```bash
plantuml -tsvg docs/diagrams/*.puml
```
