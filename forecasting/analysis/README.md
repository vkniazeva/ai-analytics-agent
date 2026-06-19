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
2. **Potential Errors Filtered**: ~10 records flagged as _no_pax_data_ (new flights without passenger history) 
and about 10 records were marked as zero_pax_count (potentially a data quality issue) were excluded from training dataset.
3. **Missing Value Check**: No missing values in analysis dataset (post-filtering)

---

## Data Statistics

### Feature Summary

| Column                   | Data Type | Nulls  | Unique Values | Description                                                                    |
|--------------------------|-----------|--------|---------------|--------------------------------------------------------------------------------|
| **flight_key**           | str       | 0      | 6,193         | Unique flight instance identifier (surrogate key: flight_number + date + hour) |
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
| **number_of_passengers** | float     | 0      | 187           | Total passengers on flight (summed across all travel classes)                  |
| **item_id**              | str       | 0      | 10            | Product identifier (Fresh Product SKU)                                         |
| **category**             | str       | 0      | 6             | Product category (Bakery, Sandwiches, etc.)                                    |
| **price**                | float     | 0      | 7             | Product unit price                                                             |
| **sold_quantity**        | float     | 0      | 43            | **TARGET**: Number of units sold (0 = no sales)                                |
| **potential_error**      | str       | 59,610 | 0             | Data quality flag (filtered out for analysis)                                  |


#### Key Observations

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
- 187 unique passenger count values
- Includes actual counts and flight-number-based averages

**Target Variable (sold_quantity):**
- 43 unique values (0 to 61 units)
- High zero-inflation (~40% zero sales)
- Right-skewed distribution (mean > median)

Conclusion: 
Based on the target variable statistics, all type of the linear models (Lasso, Ridge etc.) can be excluded 
as they don’t perform well on the zero-inflated datasets. Additionally, all types of optimizations based on MSE (mean square error) 
or RMSE (root mean square error) can’t be taken for the model development, as such algorithms put high penalties to the outlier cases.
Therefore, they will systematically lead to the overpredicts zeros and ones.


## Target Analysis (sold_quantity)

### Descriptive Statistics

| Statistic   | sold_quantity | 
|-------------|---------------|
| **count**   | 59,620        | 
| **mean**    | 1.29          | 
| **std**     | 2.18          | 
| **min**     | 0.00          | 
| **25%**     | 0.00          | 
| **50%**     | 1.00          | 
| **75%**     | 2.00          | 
| **max**     | 61.00         | 


| Feature                  | Skewness | Kurtosis | Zero Values    |
|--------------------------|----------|----------|----------------|
| **sold_quantity**        | 7.10     | 100.74   | 23,624 (39.6%) | 


Zero sales: 23624 (39.6%)
Non-zero sales: 35996 (60.4%)

Percentile distribution:
-  10%: 0.0
-  25%: 0.0
-  50%: 1.0
-  75%: 2.0
-  90%: 3.0
-  95%: 4.0
-  99%: 10.0

Distribution Characteristics:
- High variability: Standard deviation (2.18) is 170% of mean (1.29)
- Severe right skew (7.10): Long tail of high-value outliers 
- Extreme kurtosis (100.74): Very sharp peak with extremely heavy tails
- Zero-inflation: 39.6% of observations have zero sales
- Outliers right skewed: 1% of observations includes 10–61 units, forming a heavy tail that dominates the outlier structure.

Key Statistics:
- 75% of sales ≤ 2 units (highly concentrated at low values)
- Maximum of 61 units represents significant outlier
- Median (1.0) < Mean (1.29) confirms right skew

### Target outliers analysis

**Outlier Detection (IQR Method):**

Calculation:
- Q1 (25th percentile) = 0.00
- Q3 (75th percentile) = 2.00
- IQR = Q3 - Q1 = 2.00
- Lower bound = Q1 - 1.5 × IQR = 0 - 3.0 = -3.0
- Upper bound = Q3 + 1.5 × IQR = 2 + 3.0 = 5.0

**Outlier Range:** Values < -3.0 or > 5.0

**Results:**
- Total outliers detected: 1,789 (3.0% of dataset)
- All outliers are upper outliers (> 5.0 units)
- No lower outliers (minimum value is 0.00)

![outliers_box.png](assests/outliers_box.png)

**Box Plot Observations:**
- Narrow box (Q1=0, Q3=2): 50% of data concentrated in 0-2 unit range
- Short lower whisker: High concentration at zero
- Long upper whisker extending to 5.0
- Numerous points beyond upper bound: Heavy right tail with values up to 61 units

## Numerical features analysis

There are 3 numerical features present in the dataset:
- price - item price. Not a categorical feature (for 10 different products - 7 different prices)
- hour_of_departure - rounded hour value (in case of 23:30 is rounded to 23 and all other values are rounded mathematically)
- number_of_passengers - number of passengers checked in for a given flight

The dataset covers the period from November 2025 to March 2026. 
The year feature was excluded from analysis as it takes only two values whose distribution is entirely determined by the data collection timeframe and carries no independent predictive signal.

| Statistic   | price     | hour_of_departure | number_of_passengers |
|-------------|-----------|-------------------|----------------------|
| **count**   | 59,620    | 59,620            | 59,610               |
| **mean**    | 16.85     | 13.06             | 154.56               |
| **std**     | 8.18      | 6.44              | 31.11                |
| **min**     | 7.00      | 0.00              | 13.00                |
| **25%**     | 7.00      | 9.00              | 145.00               |
| **50%**     | 15.00     | 14.00             | 168.00               |
| **75%**     | 25.00     | 19.00             | 174.00               |
| **max**     | 30.00     | 23.00             | 355.00               |

### Distribution Shape Metrics

| Feature                  | Skewness | Kurtosis | Zero Values  | Interpretation                  |
|--------------------------|----------|----------|--------------|---------------------------------|
| **price**                | 0.20     | -1.27    | 0 (0.0%)     | Nearly symmetric, flat peak     |
| **hour_of_departure**    | -0.11    | -0.99    | 1,240 (2.1%) | Nearly symmetric, flat peak     |
| **number_of_passengers** | -1.08    | 4.66     | 0 (0.0%)     | Left skew, sharp peak           |


**1. price**

Distribution Characteristics:
- Moderate variability: Standard deviation (8.18) is 49% of mean (16.85)
- Nearly symmetric (skewness: 0.20): Mean ≈ Median
- Platykurtic (kurtosis: -1.27): Flat distribution, few extreme values
- No zeros: All products have positive pricing

Key Statistics:
- Price range: 7 - 30 (7 distinct price points)
- Median: 15 (midpoint of range)
- IQR: 7 - 25 (captures 50% of data)

**2. hour_of_departure**

Distribution Characteristics:
- Moderate variability: Standard deviation (6.44) is 49% of mean (13.06)
- Nearly symmetric (skewness: -0.11): Uniform flight distribution across day
- Platykurtic (kurtosis: -0.99): Flat distribution, consistent coverage
- Midnight flights: 1,240 observations (2.1%) at hour 0

Key Statistics:
- Hour range: 0-23 (full 24-hour coverage)
- Median: 14:00 (afternoon peak)
- IQR: 09:00 - 19:00 (daytime operations)

**3. number_of_passengers**

Distribution Characteristics:
- Low variability: Standard deviation (31.11) is only 20% of mean (154.56)
- Left skew (-1.08): Concentration at higher passenger counts
- Leptokurtic (4.66): Sharp peak with moderate tails
- Suspicious zeros: were excluded from the dataset

Key Statistics:
- Passenger range: 13 - 355
- Median: 168 passengers (close to mean, high load factor)
- IQR: 145 - 174 (tight clustering around high occupancy)

### Correlation Analysis

Correlation with sold_quantity:

| Metric               | Correlation |
|----------------------|-------------|
| price                | 0.15        |
| hour_of_departure    | 0.06        |
| number_of_passengers | 0.002       |
| year                 | -0.02       |


Correlation analysis revealed no meaningful linear relationship between the target and numerical features. 
Only price shows a weak positive correlation (r = 0.15), while hour_of_departure and number_of_passengers are near zero. 
However, low linear correlation does not exclude non-linear structure, which is evident across all three features.

![numerical_feature_analysis.png](assests/numerical_feature_analysis.png)
Sales by Price Point confirms that sales volume varies considerably across the seven discrete price values. 
The lowest price tier (7–15) accounts for the highest number of records and total sales,
while the mid-range values (17–25) show significantly lower activity. 
This pattern suggests a potential dependency between price groups and sales volume, which will be further verified through binning into low, medium, and high price tiers.

Average Sales by Hour reveals a clear intra-day pattern. 
Sales are relatively low during early morning hours (1–8am), rise through the midday period, and peak around 14:00–16:00, before declining again toward midnight. 
This non-uniform distribution motivates the derivation of a day_part categorical feature for further analysis.

![scatter_plot_pax_qty.png](assests/scatter_plot_pax_qty.png)
Sold Quantity vs Number of Passengers shows a positive relationship between passenger count and sales volume up to approximately 170–180 passengers, 
beyond which sales drop sharply to near zero. 
This threshold effect suggests that flight capacity alone does not drive sales — other factors likely dominate for high-capacity flights. 
The scatter plot also highlights several extreme sales values for low-to-mid capacity flights, consistent with the outlier structure identified earlier.


## Categorical Features Analysis

The categorical feature set covers item identifiers, route information (origin, destination), 
and temporal markers (day period, weekday, weekend flag, night flag). 
Cardinality is moderate across all features and presents no technical concerns. 
The weekend and weekday features provide full coverage of all calendar days as expected given the dataset timeframe.

### Sales by Item

| item_id   | avg_qty   | total_qty   | observations  | avg_price   |
|-----------|-----------|-------------|---------------|-------------|
| T3L4D007  | 4.45      | 25,471      | 5,729         | 28.0        |
| C3L2D037  | 1.28      | 7,945       | 6,193         | 7.0         |
| C3L2D041  | 1.26      | 7,792       | 6,193         | 7.0         |
| T3L4D129  | 1.16      | 6,647       | 5,729         | 15.0        |
| C3L2W121  | 1.06      | 6,579       | 6,193         | 17.0        |
| C3L2D043  | 0.93      | 5,744       | 6,193         | 7.0         |
| T3L4D008  | 0.93      | 5,323       | 5,729         | 25.0        |
| T3L4S016  | 0.74      | 4,601       | 6,193         | 15.0        |
| T3L4D127  | 0.71      | 4,052       | 5,729         | 30.0        |
| C3L2W161  | 0.44      | 2,498       | 5,729         | 20.0        |

`T3L4D007` is the clear sales leader with an average of 4.45 units per observation (including zeros), totalling 25,471 units — nearly 3x the next best item. 
Notably, this item also carries one of the highest price points (28.0), suggesting that demand is largely price-inelastic for this product. 
The zero-inclusive average of 4.45 indicates strong and consistent sales volume whenever this item is on board.

In contrast, `C3L2W161`, `T3L4D127`, and `T3L4S016` show low average quantities despite having comparable or higher observation counts, meaning these items are frequently loaded but rarely sold. 
This pattern is independent of price — the low-performing group includes both budget (7.0) and premium (30.0) items — suggesting that product type rather than price drives demand for these SKUs. 
The distribution of sales across items will be a key signal for the predictive model and warrants item-level feature engineering.


### Sales by Day Period

| day_period  | mean  | sum    | count  |
|-------------|-------|--------|--------|
| Day         | 1.72  | 28,253 | 16,415 |
| Evening     | 1.54  | 16,050 | 10,425 |
| Morning     | 1.04  | 21,449 | 20,585 |
| Night       | 0.89  | 10,900 | 12,185 |

The day period feature shows a clear relationship with sales performance. 
Daytime observations record the highest mean sold quantity (1.72), followed by Evening (1.54). 
Morning observations are the most frequent in the dataset (20,585 records) yet yield a substantially lower mean (1.04) — 
despite having nearly twice the number of observations as Evening, total morning sales exceed evening sales by only ~34%.
This gap between volume and mean performance suggests that morning flights are structurally weaker in terms of per-observation sales conversion. 
Night period records the lowest mean (0.89), consistent with reduced passenger appetite during overnight travel.


### Sales by Weekend

| is_weekend      | mean   | sum    | count   |
|-----------------|--------|--------|---------|
| False (weekday) | 1.29   | 53,744 | 41,750  |  
| True (weekend)  | 1.28   | 22,908 | 17,860  |

The weekend flag shows virtually no difference in mean sales between weekday (1.29) and weekend (1.28) observations. 
Given the absence of any meaningful signal, this feature is unlikely to contribute predictive value and may be excluded from modelling 
unless interaction effects with other features are identified at a later stage.


### Sales by Route, Origin and Destination

Analysis is based on the top 10 routes, origins, and destinations by total sales volume.

#### Top 10 Routes by Total Sales

| Route                | avg_qty_per_flight  | total_qty   | num_flights   |
|----------------------|---------------------|-------------|---------------|
| city_017 -> city_001 | 28.64               | 3,494       | 122           |
| city_001 -> city_017 | 28.72               | 3,446       | 120           |
| city_002 -> city_001 | 9.67                | 3,125       | 323           |
| city_001 -> city_002 | 9.65                | 3,125       | 324           |
| city_001 -> city_003 | 25.14               | 3,042       | 121           |
| city_003 -> city_001 | 24.97               | 3,021       | 121           |
| city_008 -> city_001 | 10.25               | 2,861       | 279           |
| city_001 -> city_008 | 10.28               | 2,836       | 276           |
| city_001 -> city_013 | 26.65               | 2,372       | 89            |
| city_013 -> city_001 | 26.46               | 2,355       | 89            |

Route-level analysis reveals considerable variation in both total and average sales across directions.  
The `city_017 <-> city_001` corridor records the highest average sales per flight (~28.7 units) with symmetric volume in both directions, 
suggesting a consistently high-demand route. Similarly, `city_003 <-> city_001` and `city_013 <-> city_001` show high per-flight averages (25+ units), 
though the latter operates with the fewest flights among the top 10 — its historical performance warrants further investigation to determine how recently this route was introduced.

In contrast, `city_002 <-> city_001` is the most frequently operated route in the dataset yet records the lowest average sales per flight (~9.7 units), 
suggesting a high proportion of zero-sale observations for this direction. 
The `city_008 <-> city_001` corridor shows a similar pattern with moderate frequency and low per-flight average.

All top routes appear as symmetric pairs (outbound and return), with near-identical sales figures in both directions — consistent with catering being loaded at the hub airport `city_001` for both segments.

#### Top Origins and Destinations

Origins and destinations show symmetric patterns, which is expected given the hub-and-spoke structure centered on `city_001`. 
This city dominates both origin and destination rankings by total volume (38,365 and 38,369 units respectively) due to its role as the primary catering hub. 
Among non-hub cities, `city_017`, `city_003`, and `city_013` lead in average sales per flight, while `city_002` again shows high total volume but low per-flight average — consistent with the route-level findings above.

> **Note on dataset structure:** The current dataset is aggregated at the individual flight level. 
> If model performance is insufficient, an alternative aggregation at the route level (combining outbound and return segments as a single catering line) may be considered, 
> reflecting the operational reality that catering for both directions is planned simultaneously at the hub.


