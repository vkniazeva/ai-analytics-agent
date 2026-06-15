# Fresh Food Sales Forecasting - Exploratory Data Analysis

## Overview

This analysis examines fresh food sales patterns on flights to develop a demand forecasting model. The dataset combines flight operational data with product sales information, capturing the relationship between temporal, route, and product characteristics and sales outcomes.

## Dataset

**Source**: `mart.mart_fresh_food_order_sale`  
**Grain**: One row per flight-product combination  
**Time Period**: 4 months (limited seasonality analysis)  
**Key Features**:
- **Target Variable**: `sold_quantity` - number of items sold per flight-product combination
- **Temporal**: date, weekday, hour of departure, time period (morning/afternoon/evening/night)
- **Route**: origin, destination, flight number
- **Product**: item_id, price
- **Operational**: Flight-specific metadata

## Analysis Structure

### 1. Data Quality Validation
**Objective**: Ensure data integrity and identify structural issues

**Checks performed**:
- Missing value analysis
- Duplicate detection at row and grain level
- Data type validation
- Cardinality assessment

**Expected Outcome**: Clean dataset with confirmed grain (flight_key + item_id) and documented quality issues.

---

### 2. Descriptive Statistics
**Objective**: Understand central tendencies and distributions

**Metrics**:
- Summary statistics (mean, median, std, min, max) for numerical features
- Distribution shape metrics (skewness, kurtosis)
- Zero-value prevalence analysis

**Key Insights**:
- Identification of right-skewed distributions typical in retail demand
- Prevalence of zero sales (zero-inflation detection)

---

### 3. Outlier Analysis
**Objective**: Detect and characterize extreme values

**Methods**:
- IQR (Interquartile Range) method for outlier detection
- Boxplot and histogram visualizations
- Outlier prevalence quantification

**Decision Points**:
- Whether outliers represent legitimate high-demand scenarios or data errors
- Impact on model selection (robust vs. sensitive to outliers)

---

### 4. Target Analysis (Sold Quantity)
**Objective**: Deep-dive into the dependent variable

**Analysis**:
- Distribution characteristics (mean, median, variance)
- Zero-inflation rate (percentage of zero sales)
- Percentile analysis (10th, 25th, 50th, 75th, 90th, 95th, 99th)

**Implications**:
- High zero-inflation → Consider two-stage models (classification + regression)
- Distribution shape → Informs model family selection (Poisson, Negative Binomial, etc.)

---

### 5. Numerical Feature Analysis
**Objective**: Assess relationship between continuous predictors and sales

**Features Analyzed**:
- `price`: Product pricing
- `hour_of_departure`: Flight departure time
- `year`: Temporal trend (limited by 4-month window)

**Methods**:
- Correlation analysis with target
- Grouped aggregations (mean sales by price point, by hour)
- Visual inspection of relationships

**Expected Patterns**:
- Price elasticity effects (higher prices → lower demand, or premium positioning)
- Time-of-day patterns (breakfast vs. lunch vs. dinner flights)

---

### 6. Categorical Feature Analysis
**Objective**: Identify categorical predictors with strong discriminatory power

**Features Analyzed**:
- `item_id`: Product-specific demand patterns
- `day_period`: Morning/Afternoon/Evening/Night
- `weekday_name`: Day-of-week effects
- `is_weekend`: Weekend vs. weekday behavior
- `is_night`: Night flight indicator

**Metrics**:
- Cardinality (number of unique values)
- Sales by category (mean, sum, count)
- Category importance ranking

**Key Findings**:
- Product heterogeneity (which items drive volume)
- Day-of-week seasonality
- Weekend effects

---

### 7. Route Analysis (Origin-Destination)
**Objective**: Quantify route-specific demand patterns

**Analysis**:
- Top routes by total sales volume
- Top origins and destinations
- Route-level sales statistics

**Strategic Insights**:
- High-volume routes may benefit from dedicated models
- Route-specific product preferences
- Opportunity for route-based feature engineering

---

### 8. Binning Analysis
**Objective**: Test discretization strategies for continuous features

**Binning Strategies**:
- **Hour binning**: 0-6 (Night), 6-12 (Morning), 12-18 (Afternoon), 18-24 (Evening)
- **Price binning**: Quantile-based (Low/Medium/High)

**Purpose**:
- Simplify model interpretation
- Capture non-linear relationships
- Compare binned vs. continuous feature performance

---

### 9. Time-Based Analysis
**Objective**: Identify temporal trends and patterns

**Analyses**:
- Daily sales trends (total, average, observation count)
- Weekday patterns
- Month-over-month trends (limited to 4 months)

**Visualizations**:
- Time series plots of daily aggregates
- Weekday and monthly bar charts

**Limitations**:
- 4-month window insufficient for full seasonality analysis
- No year-over-year comparison available

---

### 10. Initial Feature Selection Conclusions

#### High-Importance Features
- **`item_id`**: Strong product-level heterogeneity
- **`price`**: Direct demand driver
- **`route` (origin + destination)**: Route-specific patterns
- **`weekday_name` / `is_weekend`**: Day-of-week effects
- **`day_period` / `hour_of_departure`**: Time-of-day patterns

#### Medium-Importance Features
- **`month_name`**: Limited signal due to short time window
- **`is_night`**: Overlaps with `day_period`

#### Low-Importance Features
- **`year`**: Only 2 unique values
- **`flight_number`**: High cardinality without clear pattern

---

## Key Insights

### 1. Zero-Inflation Challenge
A significant proportion of flight-product combinations result in zero sales. This suggests:
- **Two-stage modeling approach**: 
  1. Classification model (will sell vs. won't sell)
  2. Regression model (how much will sell, given non-zero)
- Alternative: Zero-Inflated Poisson (ZIP) or Zero-Inflated Negative Binomial (ZINB) models

### 2. Route Dependency
Sales patterns vary significantly by route, indicating:
- Route should be a key feature (or interaction term)
- Consider route-specific models for high-volume routes
- Feature engineering: route popularity, route type (domestic/international)

### 3. Temporal Patterns
Clear time-based effects observed:
- Day-of-week variations (weekday vs. weekend)
- Hour-of-day patterns (meal timing effects)
- Recommendation: Include temporal features, consider time-series methods if forecasting future dates

### 4. Product Heterogeneity
Items exhibit different demand patterns:
- Item-specific models may outperform global models
- Consider hierarchical models (item-level variance)
- Product clustering based on sales similarity

---

## Recommended Next Steps

### 1. Feature Engineering
- **Lag features**: Previous flight sales, moving averages
- **Route features**: Route popularity, average route demand
- **Interaction terms**: price × day_period, route × item_id
- **Categorical encoding**: Target encoding for high-cardinality features

### 2. Model Development Strategy

**Baseline Models**:
- Mean/median baseline by product
- Product × route historical average

**Statistical Models**:
- Poisson regression (count data)
- Negative Binomial regression (overdispersion)
- Zero-Inflated Poisson/Negative Binomial (zero-inflation)

**Machine Learning Models**:
- LightGBM/XGBoost (handle non-linearity, interactions)
- Random Forest (robust to outliers)

**Hierarchical Approach**:
- Product-level models with shared parameters
- Route-level models

### 3. Validation Strategy
- **Train/Test Split**: Temporal split (e.g., last 2 weeks as test)
- **Cross-Validation**: Time-series CV with expanding window
- **Metrics**: 
  - MAE, RMSE for regression performance
  - Accuracy, F1 for zero-classification
  - Custom business metrics (e.g., inventory optimization)

### 4. Deployment Considerations
- **Forecast Horizon**: Daily, weekly, or per-flight
- **Update Frequency**: Daily retraining vs. weekly
- **Feature Availability**: Ensure all features available at prediction time
- **Monitoring**: Track prediction error, data drift

---

## Files in This Directory

- **`01_data_exploration.ipynb`**: Complete EDA notebook with all 10 analysis sections
- **`README.md`**: This document - analysis methodology and findings summary

---

## Limitations & Caveats

1. **Short Time Window**: Only 4 months of data limits seasonality analysis
2. **No External Factors**: Weather, holidays, events not captured
3. **Grain Assumption**: Assumes flight_key + item_id uniqueness (validate in production)
4. **Causality**: Correlations observed do not imply causation
5. **Data Recency**: Analysis based on specific time period; patterns may change

---

## Contact & Maintenance

**Last Updated**: 2026-06-15  
**Data Source**: `mart.mart_fresh_food_order_sale`  
**Analysis Period**: 2025-11-01 onwards (4 months)

For questions or updates to this analysis, refer to the forecasting module documentation.
