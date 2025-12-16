# Task 3: Apache Spark Analytics and Machine Learning

## Sri Lanka Weather Analytics Platform

This document presents the step-by-step implementation of Spark analytics and machine learning for analyzing Sri Lanka's historical weather data (2010 - June 2024).

---

## Step 1: Setting Up Spark Session and Loading Data

### 1.1 Create Spark Session

First, we create and configure a Spark session with appropriate settings for weather data analysis.

#### Code Listing: spark_data_loader.py (Session Setup)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, StringType
)
from pyspark.sql.functions import col, to_date, year, month, dayofmonth

def create_spark_session(app_name: str = "SriLankaWeatherAnalytics") -> SparkSession:
    """Create and configure a Spark session for weather analytics."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.session.timeZone", "Asia/Colombo") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Create session
spark = create_spark_session()
print(f"Spark version: {spark.version}")
```

#### Screenshot Placeholder: Spark Session Creation
```
============================================================
Sri Lanka Weather Analytics - Spark Data Loader
============================================================

1. Creating Spark session...
   Spark version: 3.5.0
```

### 1.2 Define Data Schema

#### Code Listing: Schema Definition

```python
def get_weather_schema() -> StructType:
    """Define the schema for weather data CSV."""
    return StructType([
        StructField("location_id", IntegerType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("weather_code", IntegerType(), nullable=True),
        StructField("temperature_2m_max", FloatType(), nullable=True),
        StructField("temperature_2m_min", FloatType(), nullable=True),
        StructField("temperature_2m_mean", FloatType(), nullable=True),
        StructField("apparent_temperature_max", FloatType(), nullable=True),
        StructField("apparent_temperature_min", FloatType(), nullable=True),
        StructField("apparent_temperature_mean", FloatType(), nullable=True),
        StructField("daylight_duration", FloatType(), nullable=True),
        StructField("sunshine_duration", FloatType(), nullable=True),
        StructField("precipitation_sum", FloatType(), nullable=True),
        StructField("rain_sum", FloatType(), nullable=True),
        StructField("precipitation_hours", FloatType(), nullable=True),
        StructField("wind_speed_10m_max", FloatType(), nullable=True),
        StructField("wind_gusts_10m_max", FloatType(), nullable=True),
        StructField("wind_direction_10m_dominant", FloatType(), nullable=True),
        StructField("shortwave_radiation_sum", FloatType(), nullable=True),
        StructField("et0_fao_evapotranspiration", FloatType(), nullable=True),
        StructField("sunrise", StringType(), nullable=True),
        StructField("sunset", StringType(), nullable=True)
    ])
```

### 1.3 Load Weather Data

#### Code Listing: Data Loading with Date Parsing

```python
def load_weather_data(spark: SparkSession, file_path: str):
    """Load weather data from CSV with schema validation and null handling."""
    weather_df = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("nullValue", "") \
        .option("nanValue", "NaN") \
        .schema(get_weather_schema()) \
        .csv(file_path)
    
    # Parse date from M/D/YYYY format
    weather_df = weather_df.withColumn(
        "parsed_date",
        to_date(col("date"), "M/d/yyyy")
    )
    
    # Extract date components
    weather_df = weather_df \
        .withColumn("year", year(col("parsed_date"))) \
        .withColumn("month", month(col("parsed_date"))) \
        .withColumn("day", dayofmonth(col("parsed_date")))
    
    return weather_df

# Load data
weather_df = load_weather_data(spark, "dataset/weatherData.csv")
print(f"Total records: {weather_df.count()}")
```

#### Screenshot Placeholder: Data Loading Output
```
2. Loading weather data...
   Total records: 132,678

Schema:
root
 |-- location_id: integer (nullable = false)
 |-- date: string (nullable = false)
 |-- temperature_2m_max: float (nullable = true)
 |-- temperature_2m_mean: float (nullable = true)
 |-- precipitation_hours: float (nullable = true)
 |-- shortwave_radiation_sum: float (nullable = true)
 |-- et0_fao_evapotranspiration: float (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 ...
```

---

## Step 2: Shortwave Radiation Analysis (Requirement 4.1)

### 2.1 Objective

Calculate the percentage of days where shortwave_radiation_sum exceeds 15 MJ/m² per month across all districts.

### 2.2 Implementation

#### Code Listing: shortwave_radiation_analysis.py

```python
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, month,
    round as spark_round
)

# Radiation threshold in MJ/m²
RADIATION_THRESHOLD = 15.0

def calculate_radiation_percentage_by_month(weather_df, threshold=15.0):
    """
    Calculate the percentage of days with shortwave_radiation_sum > threshold per month.
    
    Validates: Requirements 4.1
    """
    # Filter out records with null radiation values
    valid_radiation_df = weather_df.filter(
        col("shortwave_radiation_sum").isNotNull()
    )
    
    # Calculate days above threshold indicator
    radiation_analysis = valid_radiation_df.withColumn(
        "above_threshold",
        when(col("shortwave_radiation_sum") > threshold, 1).otherwise(0)
    )
    
    # Group by month and calculate statistics
    monthly_stats = radiation_analysis.groupBy("month").agg(
        count("*").alias("total_days"),
        spark_sum("above_threshold").alias("days_above_threshold")
    )
    
    # Calculate percentage
    result = monthly_stats.withColumn(
        "percentage",
        spark_round(
            (col("days_above_threshold") / col("total_days")) * 100,
            2
        )
    )
    
    return result.orderBy("month")

# Execute analysis
results_df = calculate_radiation_percentage_by_month(weather_df)
results_df.show()
```

### 2.3 Final Output

| Month | Month Name | Total Days | Days Above 15 MJ/m² | Percentage |
|-------|------------|------------|---------------------|------------|
| 1 | January | 12,555 | 9,900 | 78.85% |
| 2 | February | 11,448 | 10,276 | 89.76% |
| 3 | March | 12,555 | 12,084 | **96.25%** |
| 4 | April | 12,150 | 11,611 | **95.56%** |
| 5 | May | 12,555 | 10,672 | 85.00% |
| 6 | June | 11,556 | 10,217 | 88.41% |
| 7 | July | 11,718 | 10,470 | 89.35% |
| 8 | August | 11,718 | 10,369 | 88.49% |
| 9 | September | 11,340 | 9,743 | 85.92% |
| 10 | October | 11,718 | 8,918 | 76.11% |
| 11 | November | 11,340 | 6,954 | 61.32% |
| 12 | December | 11,718 | 6,946 | **59.28%** |

#### Screenshot Placeholder: Radiation Analysis Output
```
======================================================================
Shortwave Radiation Analysis - Days with radiation > 15.0 MJ/m²
======================================================================
Month        Total Days   Days Above      Percentage
----------------------------------------------------------------------
January          12555           9900        78.85%
February         11448          10276        89.76%
March            12555          12084        96.25%
April            12150          11611        95.56%
May              12555          10672        85.00%
June             11556          10217        88.41%
July             11718          10470        89.35%
August           11718          10369        88.49%
September        11340           9743        85.92%
October          11718           8918        76.11%
November         11340           6954        61.32%
December         11718           6946        59.28%
======================================================================
```

**Interpretation:** March and April have the highest percentage of high-radiation days (>95%), while December has the lowest (59.28%), corresponding to the monsoon season with increased cloud cover.

---

## Step 3: Weekly Maximum Temperature Analysis (Requirements 4.2, 4.3)

### 3.1 Objective

1. Identify the hottest months based on average temperature_2m_max
2. Calculate weekly maximum temperatures within those months

### 3.2 Step 3a: Identify Hottest Months

#### Code Listing: Hottest Months Identification

```python
from pyspark.sql.functions import avg, dense_rank
from pyspark.sql.window import Window

def identify_hottest_months(weather_df, top_n=3):
    """
    Identify the hottest months based on average temperature_2m_max.
    
    Validates: Requirements 4.3
    """
    # Filter out records with null temperature values
    valid_temp_df = weather_df.filter(col("temperature_2m_max").isNotNull())
    
    # Calculate average max temperature by month
    monthly_avg = valid_temp_df.groupBy("month").agg(
        spark_round(avg("temperature_2m_max"), 2).alias("avg_max_temperature")
    )
    
    # Rank months by average max temperature
    window_spec = Window.orderBy(col("avg_max_temperature").desc())
    ranked_months = monthly_avg.withColumn("rank", dense_rank().over(window_spec))
    
    # Get top N hottest months
    hottest_months = ranked_months.filter(col("rank") <= top_n) \
        .select("month", "avg_max_temperature", "rank") \
        .orderBy("rank")
    
    return hottest_months

# Execute
hottest_months_df = identify_hottest_months(weather_df, top_n=3)
hottest_months_df.show()
```

#### Output: Hottest Months

| Rank | Month | Month Name | Avg Max Temp (°C) |
|------|-------|------------|-------------------|
| 1 | 4 | April | 30.77 |
| 2 | 3 | March | 30.36 |
| 3 | 5 | May | 30.20 |

#### Screenshot Placeholder: Hottest Months Output
```
============================================================
Hottest Months by Average Maximum Temperature
============================================================
Rank   Month        Avg Max Temp (°C)
------------------------------------------------------------
1      April                    30.77
2      March                    30.36
3      May                      30.20
============================================================
```

### 3.3 Step 3b: Calculate Weekly Maximum Temperatures

#### Code Listing: Weekly Max Temperature Calculation

```python
from pyspark.sql.functions import max as spark_max, weekofyear

def calculate_weekly_max_temperatures(weather_df, hottest_months_df):
    """
    Calculate weekly maximum temperatures for the hottest months.
    
    Validates: Requirements 4.2
    """
    # Get list of hottest month numbers
    hottest_month_list = [row["month"] for row in hottest_months_df.collect()]
    
    # Filter weather data to only include hottest months
    filtered_df = weather_df.filter(
        (col("month").isin(hottest_month_list)) & 
        (col("temperature_2m_max").isNotNull())
    )
    
    # Add week number
    filtered_df = filtered_df.withColumn("week", weekofyear(col("parsed_date")))
    
    # Calculate weekly maximum temperatures
    weekly_max = filtered_df.groupBy("year", "month", "week").agg(
        spark_round(spark_max("temperature_2m_max"), 2).alias("weekly_max_temperature")
    )
    
    return weekly_max.orderBy("year", "month", "week")

# Execute
weekly_max_df = calculate_weekly_max_temperatures(weather_df, hottest_months_df)
weekly_max_df.show(20)
```

### 3.4 Final Output: Weekly Maximum Temperatures (Sample)

| Year | Month | Week | Max Temp (°C) |
|------|-------|------|---------------|
| 2010 | 3 | 9 | 36.1 |
| 2010 | 3 | 10 | 36.7 |
| 2010 | 3 | 11 | 37.2 |
| 2010 | 3 | 12 | 36.6 |
| 2010 | 3 | 13 | 33.9 |
| 2010 | 4 | 13 | 36.1 |
| 2010 | 4 | 14 | 35.8 |
| 2010 | 4 | 15 | 36.3 |
| 2010 | 4 | 16 | 35.3 |
| 2010 | 4 | 17 | 33.3 |
| 2010 | 5 | 17 | 32.5 |
| 2010 | 5 | 18 | 33.8 |
| 2010 | 5 | 19 | 34.6 |
| 2010 | 5 | 20 | 35.0 |
| 2010 | 5 | 21 | 35.5 |
| ... | ... | ... | ... |

#### Summary Statistics

| Metric | Value |
|--------|-------|
| Overall Maximum Temperature | 40.3°C |
| Overall Average Weekly Max | 35.43°C |
| Total Weeks Analyzed | 235 |
| Year Range | 2010 - 2024 |

#### Screenshot Placeholder: Weekly Max Temperature Output
```
======================================================================
Weekly Maximum Temperatures for Hottest Months
======================================================================
Year   Month        Week    Max Temp (°C)
----------------------------------------------------------------------
2010   March           9           36.10
2010   March          10           36.70
2010   March          11           37.20
2010   March          12           36.60
2010   March          13           33.90
2010   April          13           36.10
2010   April          14           35.80
2010   April          15           36.30
...
======================================================================

Summary Statistics:
   Year range: 2010 - 2024
   Total weeks analyzed: 235
   Overall max temperature: 40.3°C
   Overall avg weekly max: 35.43°C
```

---

## Step 4: Machine Learning - May Evapotranspiration Prediction (Requirements 5.1-5.5)

### 4.1 Objective

Predict weather conditions that lead to evapotranspiration (ET0) below 1.5mm in May, using:
- Features: precipitation_hours, sunshine_duration, wind_speed_10m_max
- Target: et0_fao_evapotranspiration

### 4.2 Step 4a: Filter May Data and Select Features

#### Code Listing: ML Dataset Preparation

```python
from pyspark.sql.functions import isnan, isnull, mean as spark_mean

# Feature columns (Requirements 5.2)
FEATURE_COLUMNS = ["precipitation_hours", "sunshine_duration", "wind_speed_10m_max"]
TARGET_COLUMN = "et0_fao_evapotranspiration"
MAY_MONTH = 5

def filter_may_data(df):
    """Filter the weather DataFrame to include only May months."""
    return df.filter(col("month") == MAY_MONTH)

def select_ml_columns(df):
    """Select only the columns needed for ML."""
    columns_to_select = ["location_id", "year"] + FEATURE_COLUMNS + [TARGET_COLUMN]
    return df.select(*columns_to_select)

# Filter for May
may_df = filter_may_data(weather_df)
ml_df = select_ml_columns(may_df)
print(f"May records: {ml_df.count()}")
```

#### Screenshot Placeholder: May Data Filtering
```
5. Preparing ML dataset for May evapotranspiration prediction...

6. Dataset Statistics:
   Total records: 12,555
   Null counts after imputation: {'precipitation_hours': 0, 'sunshine_duration': 0, 
                                   'wind_speed_10m_max': 0, 'et0_fao_evapotranspiration': 0}

7. Validating ML dataset...
   Is valid: True
```

### 4.3 Step 4b: Handle Missing Values with Mean Imputation

#### Code Listing: Mean Imputation

```python
def impute_missing_values_manual(df, columns):
    """Impute missing values (null and NaN) with column means."""
    # Calculate means for all columns
    means = {}
    for column in columns:
        mean_value = df.filter(
            ~(isnan(col(column)) | isnull(col(column)))
        ).agg(spark_mean(col(column))).collect()[0][0]
        means[column] = mean_value if mean_value is not None else 0.0
    
    # Replace nulls and NaNs with means
    result_df = df
    for column in columns:
        mean_value = means[column]
        result_df = result_df.withColumn(
            column,
            when(
                isnan(col(column)) | isnull(col(column)),
                mean_value
            ).otherwise(col(column))
        )
    
    return result_df

# Impute missing values
columns_to_impute = FEATURE_COLUMNS + [TARGET_COLUMN]
ml_df_imputed = impute_missing_values_manual(ml_df, columns_to_impute)
```

#### Screenshot Placeholder: Imputation Output
```
9. Feature Statistics:
+-------+-------------------+------------------+------------------+--------------------------+
|summary|precipitation_hours| sunshine_duration|wind_speed_10m_max|et0_fao_evapotranspiration|
+-------+-------------------+------------------+------------------+--------------------------+
|  count|              12555|             12555|             12555|                     12555|
|   mean|  8.011867781760255|34302.532460807546|17.848936683236246|         4.225671845581939|
| stddev|  6.946432602146943| 9210.728019888933| 7.709660926829211|        1.1799446226009025|
|    min|                0.0|               0.0|               2.5|        0.5899999737739563|
|    max|               24.0|      42149.109375| 48.29999923706055|         8.449999809265137|
+-------+-------------------+------------------+------------------+--------------------------+

   Null values after imputation: 0
```

### 4.4 Step 4c: Train/Validation Split (80/20)

#### Code Listing: Dataset Split

```python
# Constants (Requirements 5.1)
TRAIN_RATIO = 0.8
VALIDATION_RATIO = 0.2
DEFAULT_SEED = 42

def split_train_validation(df, train_ratio=0.8, validation_ratio=0.2, seed=42):
    """
    Split the dataset into training and validation sets.
    
    Requirements: 5.1
    """
    train_df, validation_df = df.randomSplit(
        weights=[train_ratio, validation_ratio],
        seed=seed
    )
    return train_df, validation_df

# Split data
train_df, validation_df = split_train_validation(ml_df_imputed)

# Verify split ratios
total_count = ml_df_imputed.count()
train_count = train_df.count()
validation_count = validation_df.count()

print(f"Total records: {total_count}")
print(f"Training records: {train_count} ({train_count/total_count*100:.1f}%)")
print(f"Validation records: {validation_count} ({validation_count/total_count*100:.1f}%)")
```

#### Screenshot Placeholder: Split Verification
```
10. Splitting dataset into training and validation sets...
    Total records: 12,555
    Training records: 10,127
    Validation records: 2,428
    Random seed: 42

    Split Verification:
    - Actual train ratio: 0.8066
    - Actual validation ratio: 0.1934
    - Expected train ratio: 0.8
    - Deviation: 0.0066
    - Within tolerance (±0.01): True
    - Split verification PASSED: Training ratio 0.8066 is within ±0.01 of expected 0.8
```

### 4.5 Step 4d: Train Regression Models

#### Code Listing: Linear Regression Training

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def create_feature_vector(df, feature_cols):
    """Create a feature vector column from individual feature columns."""
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    return assembler.transform(df)

def train_linear_regression(train_df, feature_cols, target_col, 
                           max_iter=100, reg_param=0.01):
    """
    Train a Linear Regression model.
    
    Requirements: 5.3
    """
    # Create feature vector
    train_with_features = create_feature_vector(train_df, feature_cols)
    
    # Configure Linear Regression
    lr = LinearRegression(
        featuresCol="features",
        labelCol=target_col,
        predictionCol="prediction",
        maxIter=max_iter,
        regParam=reg_param,
        standardization=True,
        fitIntercept=True
    )
    
    # Train the model
    lr_model = lr.fit(train_with_features)
    
    return {
        "model": lr_model,
        "coefficients": dict(zip(feature_cols, lr_model.coefficients.toArray())),
        "intercept": lr_model.intercept,
        "training_summary": {
            "rmse": lr_model.summary.rootMeanSquaredError,
            "r2": lr_model.summary.r2
        }
    }

# Train Linear Regression
lr_result = train_linear_regression(train_df, FEATURE_COLUMNS, TARGET_COLUMN)
print(f"Linear Regression trained successfully")
print(f"Coefficients: {lr_result['coefficients']}")
print(f"Intercept: {lr_result['intercept']:.4f}")
```

#### Screenshot Placeholder: Model Training Output
```
============================================================
13. Training Regression Models
============================================================

13.1 Training Linear Regression model...

============================================================
Model: LinearRegression
============================================================

Features: ['precipitation_hours', 'sunshine_duration', 'wind_speed_10m_max']
Target: et0_fao_evapotranspiration

Hyperparameters:
  - max_iter: 100
  - reg_param: 0.01
  - elastic_net_param: 0.0

Model Coefficients:
  - precipitation_hours: -0.079909
  - sunshine_duration: 0.000057
  - wind_speed_10m_max: 0.042039
  - intercept: 2.165706

Training Metrics:
  - RMSE: 0.5407
  - R²: 0.7896
  - MAE: 0.4208
  - Iterations: 0
```

### 4.6 Step 4e: Evaluate Model on Validation Set

#### Code Listing: Model Evaluation

```python
def evaluate_model(model_result, validation_df):
    """
    Evaluate a trained model on the validation dataset.
    
    Requirements: 5.4
    """
    model = model_result["model"]
    feature_cols = FEATURE_COLUMNS
    target_col = TARGET_COLUMN
    
    # Create feature vector for validation data
    validation_with_features = create_feature_vector(validation_df, feature_cols)
    
    # Make predictions
    predictions_df = model.transform(validation_with_features)
    
    # Calculate RMSE
    rmse_evaluator = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="rmse"
    )
    rmse = rmse_evaluator.evaluate(predictions_df)
    
    # Calculate R-squared
    r2_evaluator = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="r2"
    )
    r2 = r2_evaluator.evaluate(predictions_df)
    
    return {"rmse": rmse, "r2": r2, "predictions_df": predictions_df}

# Evaluate model
lr_eval = evaluate_model(lr_result, validation_df)
print(f"Validation RMSE: {lr_eval['rmse']:.4f}")
print(f"Validation R²: {lr_eval['r2']:.4f}")
```

#### Final Output: Model Evaluation Metrics

| Model | RMSE | R² | MAE |
|-------|------|-----|-----|
| Linear Regression (Training) | 0.5407 | 0.7896 | 0.4208 |
| Linear Regression (Validation) | 0.5361 | 0.7950 | 0.4242 |
| Random Forest (Validation) | 0.5015 | 0.8206 | 0.3912 |

#### Screenshot Placeholder: Evaluation Output
```
============================================================
14. Model Comparison (Requirements 5.4)
============================================================

Model                     RMSE         R²           MAE         
------------------------------------------------------------
Linear Regression         0.5361       0.7950       0.4242      
Random Forest             0.5015       0.8206       0.3912      

Best model based on RMSE: Random Forest

Model Performance Summary:
  The Random Forest model explains approximately 82% of the variance in evapotranspiration.
  Average prediction error is about 0.50mm.
```

### 4.7 Step 4f: Predict Conditions for May 2026 with ET0 < 1.5mm

#### Code Listing: Prediction for Low ET0

```python
def predict_conditions_for_low_et0(model_result, train_df, target_et0=1.5):
    """
    Predict weather conditions that would result in ET0 below threshold.
    
    Requirements: 5.5
    """
    # Find historical records with ET0 below threshold
    low_et0_records = train_df.filter(col(TARGET_COLUMN) < target_et0)
    low_et0_count = low_et0_records.count()
    
    if low_et0_count > 0:
        # Calculate mean feature values from historical low ET0 records
        feature_means = low_et0_records.agg(
            avg(col("precipitation_hours")).alias("mean_precipitation_hours"),
            avg(col("sunshine_duration")).alias("mean_sunshine_duration"),
            avg(col("wind_speed_10m_max")).alias("mean_wind_speed"),
            avg(col(TARGET_COLUMN)).alias("mean_et0")
        ).collect()[0]
        
        return {
            "predicted_precipitation_hours": round(feature_means["mean_precipitation_hours"], 2),
            "predicted_sunshine_duration": round(feature_means["mean_sunshine_duration"], 2),
            "predicted_wind_speed": round(feature_means["mean_wind_speed"], 2),
            "historical_mean_et0": round(feature_means["mean_et0"], 4),
            "historical_sample_count": low_et0_count,
            "target_et0_threshold": target_et0
        }

# Predict conditions
prediction = predict_conditions_for_low_et0(lr_result, train_df)
print(f"Predicted conditions for May 2026 with ET0 < 1.5mm:")
print(f"  Precipitation hours: {prediction['predicted_precipitation_hours']} hours")
print(f"  Sunshine duration: {prediction['predicted_sunshine_duration']} seconds")
print(f"  Wind speed: {prediction['predicted_wind_speed']} km/h")
```

#### Final Output: May 2026 Prediction

| Parameter | Predicted Value | Unit |
|-----------|-----------------|------|
| Precipitation Hours | 22.25 | hours |
| Sunshine Duration | 1,525.14 | seconds (~0.42 hours) |
| Wind Speed (10m max) | 18.17 | km/h |
| Target ET0 | < 1.5 | mm |
| Model Predicted ET0 | 2.17 | mm |

#### Screenshot Placeholder: Prediction Output
```
============================================================
16. May 2026 Prediction (Requirements 5.5)
============================================================

Using Random Forest model for May 2026 prediction...

============================================================
May 2026 Prediction for ET0 < 1.5mm
============================================================

Target: Evapotranspiration (ET0) < 1.5mm
Method: historical_analysis
Note: Based on 105 historical May records with ET0 < 1.5mm

Predicted Weather Conditions for May 2026:
  - Precipitation Hours: 22.25 hours
  - Sunshine Duration: 1525.14 seconds
  - Wind Speed (10m max): 18.17 km/h

Model Predicted ET0: 2.1677 mm

Historical Reference:
  - Mean ET0 from similar conditions: 1.1776 mm
  - Sample size: 105 records

Interpretation:
  - Higher precipitation hours (more cloud cover/rain)
  - Very low sunshine duration (heavy cloud cover)
  - Moderate wind speeds
  
These conditions typically occur during cloudy, rainy days in May.
============================================================
```

---

## Summary of Spark Analytics Results

| Analysis | Key Finding |
|----------|-------------|
| **Radiation Analysis** | March has highest % of high-radiation days (96.25%) |
| **Lowest Radiation** | December (59.28%) due to monsoon cloud cover |
| **Hottest Month** | April (30.77°C average max) |
| **Weekly Max Temp** | Overall max 40.3°C, avg 35.43°C |
| **Best ML Model** | Random Forest Regressor |
| **ML Model R²** | 0.8206 (explains 82% of ET0 variance) |
| **ML Model RMSE** | 0.5015mm prediction error |
| **May Records** | 12,555 total (10,127 train / 2,428 validation) |
| **Low ET0 Conditions** | High precipitation (22.25h), low sunshine (1525s), moderate wind (18.17 km/h) |

---

## Files Included

| File | Description |
|------|-------------|
| `spark/spark_data_loader.py` | Data loading and schema definitions |
| `spark/shortwave_radiation_analysis.py` | Radiation percentage calculation |
| `spark/weekly_max_temperature_analysis.py` | Weekly temperature analysis |
| `spark/ml_dataset_preparation.py` | ML dataset preparation and model training |
| `spark/spark_data_loader.ipynb` | Jupyter notebook version |
| `spark/spark_radiation_analysis.ipynb` | Jupyter notebook for radiation analysis |
| `spark/spark_weekly_temps.ipynb` | Jupyter notebook for temperature analysis |
| `spark/spark_ml_dataset_preparation.ipynb` | Jupyter notebook for ML |
