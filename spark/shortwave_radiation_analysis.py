"""
Sri Lanka Weather Analytics - Shortwave Radiation Analysis

This module calculates the percentage of days with shortwave_radiation_sum > 15 MJ/m²
per month across all districts.

Requirements: 4.1
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, month, year, to_date,
    coalesce, lit, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, StringType
)
import os


# Radiation threshold in MJ/m²
RADIATION_THRESHOLD = 15.0


def create_spark_session(app_name: str = "ShortwaveRadiationAnalysis") -> SparkSession:
    """Create and configure a Spark session."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.session.timeZone", "Asia/Colombo") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


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


def load_weather_data(spark: SparkSession, file_path: str) -> DataFrame:
    """Load weather data from CSV with schema validation."""
    weather_df = spark.read \
        .option("header", "true") \
        .option("mode", "PERMISSIVE") \
        .option("nullValue", "") \
        .option("nanValue", "NaN") \
        .schema(get_weather_schema()) \
        .csv(file_path)
    
    # Parse date and extract month
    weather_df = weather_df.withColumn(
        "parsed_date",
        to_date(col("date"), "M/d/yyyy")
    )
    
    weather_df = weather_df \
        .withColumn("year", year(col("parsed_date"))) \
        .withColumn("month", month(col("parsed_date")))
    
    return weather_df


def calculate_radiation_percentage_by_month(weather_df: DataFrame, threshold: float = RADIATION_THRESHOLD) -> DataFrame:
    """
    Calculate the percentage of days with shortwave_radiation_sum > threshold per month.
    
    This function groups data by month across all districts and calculates:
    - Total number of days with valid radiation readings
    - Number of days exceeding the threshold
    - Percentage of days exceeding the threshold
    
    Args:
        weather_df: DataFrame with weather data including shortwave_radiation_sum
        threshold: Radiation threshold in MJ/m² (default: 15.0)
        
    Returns:
        DataFrame with columns: month, total_days, days_above_threshold, percentage
        
    Validates: Requirements 4.1
    """
    # Filter out records with null radiation values for accurate percentage calculation
    valid_radiation_df = weather_df.filter(col("shortwave_radiation_sum").isNotNull())
    
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
    
    # Order by month
    result = result.orderBy("month")
    
    return result


def get_month_name(month_num: int) -> str:
    """Convert month number to month name."""
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    return months[month_num - 1] if 1 <= month_num <= 12 else "Unknown"


def format_results(results_df: DataFrame) -> list:
    """
    Format results as a list of dictionaries with month names.
    
    Args:
        results_df: DataFrame with monthly radiation statistics
        
    Returns:
        List of dictionaries with formatted results
    """
    rows = results_df.collect()
    formatted = []
    
    for row in rows:
        formatted.append({
            "month": row["month"],
            "month_name": get_month_name(row["month"]),
            "total_days": row["total_days"],
            "days_above_threshold": row["days_above_threshold"],
            "percentage": row["percentage"]
        })
    
    return formatted


def print_results(results: list, threshold: float = RADIATION_THRESHOLD):
    """Print formatted results to console."""
    print("\n" + "=" * 70)
    print(f"Shortwave Radiation Analysis - Days with radiation > {threshold} MJ/m²")
    print("=" * 70)
    print(f"{'Month':<12} {'Total Days':>12} {'Days Above':>15} {'Percentage':>12}")
    print("-" * 70)
    
    for r in results:
        print(f"{r['month_name']:<12} {r['total_days']:>12} {r['days_above_threshold']:>15} {r['percentage']:>11.2f}%")
    
    print("=" * 70)


# Main execution
if __name__ == "__main__":
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define data path
    weather_path = os.path.join(project_root, "dataset", "weatherData.csv")
    
    print("=" * 70)
    print("Sri Lanka Weather Analytics - Shortwave Radiation Analysis")
    print("=" * 70)
    
    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session()
    print(f"   Spark version: {spark.version}")
    
    # Load weather data
    print("\n2. Loading weather data...")
    weather_df = load_weather_data(spark, weather_path)
    print(f"   Total records: {weather_df.count()}")
    
    # Calculate radiation percentages
    print(f"\n3. Calculating percentage of days with radiation > {RADIATION_THRESHOLD} MJ/m²...")
    results_df = calculate_radiation_percentage_by_month(weather_df)
    
    # Format and display results
    results = format_results(results_df)
    print_results(results)
    
    # Show DataFrame for verification
    print("\n4. Raw DataFrame output:")
    results_df.show()
    
    # Stop Spark session
    spark.stop()
    print("\nAnalysis complete!")
