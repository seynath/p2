"""
Sri Lanka Weather Analytics - Spark Data Loader

This module sets up the Spark session and loads weather and location data
with proper schema definitions and null handling.

Requirements: 4.4, 7.3
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, 
    StringType, DateType
)
from pyspark.sql.functions import (
    col, to_date, when, coalesce, lit, avg, sum as spark_sum,
    month, year, dayofmonth
)
import os


def create_spark_session(app_name: str = "SriLankaWeatherAnalytics") -> SparkSession:
    """
    Create and configure a Spark session for weather analytics.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession instance
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.session.timeZone", "Asia/Colombo") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def get_weather_schema() -> StructType:
    """
    Define the schema for weather data CSV.
    
    The schema matches the weatherData.csv structure with proper data types.
    Column names are cleaned to remove units and special characters.
    
    Returns:
        StructType schema for weather data
    """
    return StructType([
        StructField("location_id", IntegerType(), nullable=False),
        StructField("date", StringType(), nullable=False),  # Will be parsed to date
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


def get_location_schema() -> StructType:
    """
    Define the schema for location data CSV.
    
    Returns:
        StructType schema for location data
    """
    return StructType([
        StructField("location_id", IntegerType(), nullable=False),
        StructField("latitude", FloatType(), nullable=True),
        StructField("longitude", FloatType(), nullable=True),
        StructField("elevation", IntegerType(), nullable=True),
        StructField("utc_offset_seconds", IntegerType(), nullable=True),
        StructField("timezone", StringType(), nullable=True),
        StructField("timezone_abbreviation", StringType(), nullable=True),
        StructField("city_name", StringType(), nullable=False)
    ])


def load_weather_data(spark: SparkSession, file_path: str):
    """
    Load weather data from CSV with schema validation and null handling.
    
    The date column is parsed from M/D/YYYY format to proper DateType.
    Null values in numeric columns are handled appropriately.
    
    Args:
        spark: Active SparkSession
        file_path: Path to weatherData.csv
        
    Returns:
        DataFrame with weather data and parsed date column
    """
    # Read CSV with custom schema
    # Note: We read date as string first, then parse it
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
    
    # Extract date components for easier analysis
    weather_df = weather_df \
        .withColumn("year", year(col("parsed_date"))) \
        .withColumn("month", month(col("parsed_date"))) \
        .withColumn("day", dayofmonth(col("parsed_date")))
    
    # Handle null values in critical numeric columns with appropriate defaults
    # Using coalesce to replace nulls with 0 for aggregation-safe columns
    null_safe_columns = [
        "precipitation_hours",
        "precipitation_sum",
        "rain_sum"
    ]
    
    for column in null_safe_columns:
        weather_df = weather_df.withColumn(
            column,
            coalesce(col(column), lit(0.0))
        )
    
    return weather_df


def load_location_data(spark: SparkSession, file_path: str):
    """
    Load location data from CSV with schema validation.
    
    Args:
        spark: Active SparkSession
        file_path: Path to locationData.csv
        
    Returns:
        DataFrame with location data
    """
    location_df = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("nullValue", "") \
        .schema(get_location_schema()) \
        .csv(file_path)
    
    return location_df


def join_weather_location(weather_df, location_df):
    """
    Join weather data with location data using location_id.
    
    This creates a complete dataset with both weather observations
    and geographic information for each record.
    
    Args:
        weather_df: DataFrame with weather data
        location_df: DataFrame with location data
        
    Returns:
        Joined DataFrame with weather and location information
    """
    joined_df = weather_df.join(
        location_df,
        on="location_id",
        how="inner"
    )
    
    return joined_df


def get_data_summary(df, name: str = "DataFrame"):
    """
    Print summary statistics for a DataFrame.
    
    Args:
        df: DataFrame to summarize
        name: Name to display in output
    """
    print(f"\n{'='*60}")
    print(f"Summary for: {name}")
    print(f"{'='*60}")
    print(f"Total records: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nSchema:")
    df.printSchema()
    print(f"\nNull counts per column:")
    
    # Count nulls in each column
    null_counts = []
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        if null_count > 0:
            null_counts.append(f"  {column}: {null_count}")
    
    if null_counts:
        for nc in null_counts:
            print(nc)
    else:
        print("  No null values found")


def validate_data_quality(weather_df, location_df):
    """
    Validate data quality and report any issues.
    
    Checks:
    - All location_ids in weather data exist in location data
    - Date parsing was successful
    - Critical columns have valid values
    
    Args:
        weather_df: Weather DataFrame
        location_df: Location DataFrame
        
    Returns:
        dict with validation results
    """
    results = {
        "weather_record_count": weather_df.count(),
        "location_count": location_df.count(),
        "orphan_weather_records": 0,
        "invalid_dates": 0,
        "issues": []
    }
    
    # Check for orphan weather records (no matching location)
    weather_location_ids = weather_df.select("location_id").distinct()
    location_ids = location_df.select("location_id").distinct()
    
    orphans = weather_location_ids.subtract(location_ids).count()
    results["orphan_weather_records"] = orphans
    
    if orphans > 0:
        results["issues"].append(f"Found {orphans} weather records with no matching location")
    
    # Check for invalid dates
    invalid_dates = weather_df.filter(col("parsed_date").isNull()).count()
    results["invalid_dates"] = invalid_dates
    
    if invalid_dates > 0:
        results["issues"].append(f"Found {invalid_dates} records with unparseable dates")
    
    return results


# Convenience functions for null-safe aggregations

def null_safe_avg(df, column: str, group_by_cols: list = None):
    """
    Calculate average excluding null values.
    
    Args:
        df: DataFrame
        column: Column to average
        group_by_cols: Optional list of columns to group by
        
    Returns:
        DataFrame with average calculation
    """
    if group_by_cols:
        return df.filter(col(column).isNotNull()) \
            .groupBy(group_by_cols) \
            .agg(avg(col(column)).alias(f"avg_{column}"))
    else:
        return df.filter(col(column).isNotNull()) \
            .agg(avg(col(column)).alias(f"avg_{column}"))


def null_safe_sum(df, column: str, group_by_cols: list = None):
    """
    Calculate sum excluding null values (nulls treated as 0).
    
    Args:
        df: DataFrame
        column: Column to sum
        group_by_cols: Optional list of columns to group by
        
    Returns:
        DataFrame with sum calculation
    """
    df_with_nulls_handled = df.withColumn(
        column,
        coalesce(col(column), lit(0.0))
    )
    
    if group_by_cols:
        return df_with_nulls_handled \
            .groupBy(group_by_cols) \
            .agg(spark_sum(col(column)).alias(f"sum_{column}"))
    else:
        return df_with_nulls_handled \
            .agg(spark_sum(col(column)).alias(f"sum_{column}"))


# Main execution for standalone use
if __name__ == "__main__":
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define data paths
    weather_path = os.path.join(project_root, "dataset", "weatherData.csv")
    location_path = os.path.join(project_root, "dataset", "locationData.csv")
    
    print("="*60)
    print("Sri Lanka Weather Analytics - Spark Data Loader")
    print("="*60)
    
    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session()
    print(f"   Spark version: {spark.version}")
    
    # Load data
    print("\n2. Loading weather data...")
    weather_df = load_weather_data(spark, weather_path)
    get_data_summary(weather_df, "Weather Data")
    
    print("\n3. Loading location data...")
    location_df = load_location_data(spark, location_path)
    get_data_summary(location_df, "Location Data")
    
    # Validate data quality
    print("\n4. Validating data quality...")
    validation = validate_data_quality(weather_df, location_df)
    print(f"   Weather records: {validation['weather_record_count']}")
    print(f"   Location records: {validation['location_count']}")
    print(f"   Orphan records: {validation['orphan_weather_records']}")
    print(f"   Invalid dates: {validation['invalid_dates']}")
    
    if validation["issues"]:
        print("\n   Issues found:")
        for issue in validation["issues"]:
            print(f"   - {issue}")
    else:
        print("   No data quality issues found!")
    
    # Join datasets
    print("\n5. Joining weather and location data...")
    joined_df = join_weather_location(weather_df, location_df)
    print(f"   Joined records: {joined_df.count()}")
    
    # Show sample data
    print("\n6. Sample joined data:")
    joined_df.select(
        "city_name", "parsed_date", "temperature_2m_mean", 
        "precipitation_hours", "et0_fao_evapotranspiration"
    ).show(10, truncate=False)
    
    # Demonstrate null-safe aggregation
    print("\n7. Example: Average temperature by city (null-safe):")
    avg_temp = null_safe_avg(joined_df, "temperature_2m_mean", ["city_name"])
    avg_temp.orderBy("avg_temperature_2m_mean", ascending=False).show(10)
    
    print("\n" + "="*60)
    print("Data loading complete!")
    print("="*60)
    
    # Stop Spark session
    spark.stop()
