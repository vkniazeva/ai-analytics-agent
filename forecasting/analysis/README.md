# Fresh Food Sales Forecasting - Exploratory Data Analysis

## Purpose

This exploratory data analysis (EDA) investigates fresh food sales patterns on flights to:
- Understand the statistical distribution and relationships within the data
- Identify potentially useful features for demand forecasting models
- Determine appropriate modeling approaches based on data characteristics
- Detect and document data quality issues

## Data Source

**Mart**: `mart_fresh_food_order_sale`  
**Grain**: One row per flight-product combination  
**Period**: 4 months (limited seasonality analysis)  
**Total Records**: ~59,630 (after cleaning)

**Sample Record:**

| flight_key  | flight_number | origin   | destination | date       | item_id   | category | price | sold_quantity | number_of_passengers |
|-------------|---------------|----------|-------------|------------|-----------|----------|-------|---------------|----------------------|
| c02f71...   | AB134         | city_002 | city_001    | 2025-11-02 | C3L2D037  | Bakery   | 7.0   | 0.0           |  175.0               |


## Data Cleaning

**Steps applied:**

1. **Duplicate Check**: No duplicates found at grain level (flight_key + item_id)
2. **Potential Errors Filtered**: ~10 records flagged as _no_pax_data_ (new flights without passenger history) were excluded from training dataset
3. **Missing Value Check**: No missing values in analysis dataset (post-filtering)

---

## Data Statistics

### Feature Summary

| Column                   | Data Type | Nulls  | Unique Values | Description                                                                    |
|--------------------------|-----------|--------|---------------|--------------------------------------------------------------------------------|
| **flight_key**           | str       | 0      | 6,194         | Unique flight instance identifier (surrogate key: flight_number + date + hour) |
| **flight_number**        | str       | 0      | 86            | Flight route identifier (e.g., AB134)                                          |
| **origin**               | str       | 0      | 29            | Departure airport/city code                                                    |
| **destination**          | str       | 0      | 30            | Arrival airport/city code                                                      |
| **date**                 | datetime  | 0      | 120           | Flight date (calendar day)                                                     |
| **month_name**           | str       | 0      | 4             | Month name (November, December, January, February)                             |
| **year**                 | float     | 0      | 2             | Year (2025, 2026) - limited temporal signal                                    |
| **weekday_name**         | str       | 0      | 7             | Day of week (Monday through Sunday)                                            |
| **is_weekend**           | bool      | 0      | 2             | Weekend indicator (Saturday/Sunday = True)                                     |
| **hour_of_departure**    | float     | 0      | 24            | Departure hour (0-23, rounded from departure time)                             |
| **day_period**           | str       | 0      | 4             | Time period (Night, Morning, Day, Evening)                                     |
| **is_night**             | bool      | 0      | 2             | Night flight indicator (22:00-04:59 = True)                                    |
| **am_pm**                | str       | 0      | 2             | AM/PM indicator                                                                |
| **number_of_passengers** | float     | 0      | 188           | Total passengers on flight (summed across all travel classes)                  |
| **item_id**              | str       | 0      | 10            | Product identifier (Fresh Product SKU)                                         |
| **category**             | str       | 0      | 6             | Product category (Bakery, Sandwiches, etc.)                                    |
| **price**                | float     | 0      | 7             | Product unit price                                                             |
| **sold_quantity**        | float     | 0      | 43            | **TARGET**: Number of units sold (0 = no sales)                                |
| **potential_error**      | str       | 59,620 | 0             | Data quality flag (filtered out for analysis)                                  |

### Key Observations

**Temporal Coverage:**
- 4-month analysis window (limited for seasonality detection)
- 120 unique dates
- All 7 weekdays represented

**Route Coverage:**
- 86 unique flight numbers
- 29 origin cities, 30 destination cities
- ~58 unique routes (origin-destination pairs)

**Product Coverage:**
- 10 fresh food products
- 6 product categories
- 7 distinct price points (7.0 to 30.0)

**Passenger Load:**
- 188 unique passenger count values
- Range: 0 to 355 passengers - (TO BE CHECKED ABOUT 0 CASES)
- Includes actual counts and flight-number-based averages

**Target Variable (sold_quantity):**
- 43 unique values (0 to 61 units)
- High zero-inflation (~40% zero sales)
- Right-skewed distribution (mean > median)

