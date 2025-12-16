"""
Sri Lanka Weather Analytics - Weekly Maximum Temperature Analysis

This module identifies the hottest months based on average temperature_2m_max
and calculates weekly maximum temperatures within those months.

Requirements: 4.2, 4.3
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, max as spark_max, avg, month, year, weekofyear,
    to_date, row_number, dense_rank, round as spark_round
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, StringType
)
import os


def create_spark_session(app_name: str = "WeeklyMaxTemperatureAnalysis") -> SparkSession:
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
    
    # Parse date and extract year, month, week
    weather_df = weather_df.withColumn(
        "parsed_date",
        to_date(col("date"), "M/d/yyyy")
    )
    
    weather_df = weather_df \
        .withColumn("year", year(col("parsed_date"))) \
        .withColumn("month", month(col("parsed_date"))) \
        .withColumn("week", weekofyear(col("parsed_date")))
    
    return weather_df


def identify_hottest_months(weather_df: DataFrame, top_n: int = 3) -> DataFrame:
    """
    Identify the hottest months based on average temperature_2m_max.
    
    This function calculates the average maximum temperature for each month
    across all years and districts, then identifies the top N hottest months.
    
    Args:
        weather_df: DataFrame with weather data including temperature_2m_max
        top_n: Number of hottest months to identify (default: 3)
        
    Returns:
        DataFrame with columns: month, avg_max_temperature
        
    Validates: Requirements 4.3
    """
    # Filter out records with null temperature values
    valid_temp_df = weather_df.filter(col("temperature_2m_max").isNotNull())
    
    # Calculate average max temperature by month (across all years and districts)
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


def calculate_weekly_max_temperatures(
    weather_df: DataFrame, 
    hottest_months_df: DataFrame
) -> DataFrame:
    """
    Calculate weekly maximum temperatures for the hottest months of each year.
    
    This function filters data to only include records from the hottest months,
    then calculates the maximum temperature_2m_max for each week within those months.
    
    Args:
        weather_df: DataFrame with weather data
        hottest_months_df: DataFrame with hottest months (from identify_hottest_months)
        
    Returns:
        DataFrame with columns: year, month, week, weekly_max_temperature
        
    Validates: Requirements 4.2
    """
    # Get list of hottest month numbers
    hottest_month_list = [row["month"] for row in hottest_months_df.collect()]
    
    # Filter weather data to only include hottest months
    filtered_df = weather_df.filter(
        (col("month").isin(hottest_month_list)) & 
        (col("temperature_2m_max").isNotNull())
    )
    
    # Calculate weekly maximum temperatures grouped by year, month, and week
    weekly_max = filtered_df.groupBy("year", "month", "week").agg(
        spark_round(spark_max("temperature_2m_max"), 2).alias("weekly_max_temperature")
    )
    
    # Order by year, month, week
    weekly_max = weekly_max.orderBy("year", "month", "week")
    
    return weekly_max


def get_weekly_max_with_month_info(
    weekly_max_df: DataFrame,
    hottest_months_df: DataFrame
) -> DataFrame:
    """
    Enrich weekly max temperatures with hottest month ranking information.
    
    Args:
        weekly_max_df: DataFrame with weekly max temperatures
        hottest_months_df: DataFrame with hottest months and their rankings
        
    Returns:
        DataFrame with weekly max temperatures and month ranking info
    """
    # Join to get month ranking info
    enriched_df = weekly_max_df.join(
        hottest_months_df.select("month", "avg_max_temperature", "rank"),
        on="month",
        how="inner"
    )
    
    return enriched_df.orderBy("year", "month", "week")


def get_month_name(month_num: int) -> str:
    """Convert month number to month name."""
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    return months[month_num - 1] if 1 <= month_num <= 12 else "Unknown"


def format_hottest_months_results(hottest_months_df: DataFrame) -> list:
    """Format hottest months results as a list of dictionaries."""
    rows = hottest_months_df.collect()
    formatted = []
    
    for row in rows:
        formatted.append({
            "month": row["month"],
            "month_name": get_month_name(row["month"]),
            "avg_max_temperature": row["avg_max_temperature"],
            "rank": row["rank"]
        })
    
    return formatted


def format_weekly_max_results(weekly_max_df: DataFrame) -> list:
    """Format weekly max temperature results as a list of dictionaries."""
    rows = weekly_max_df.collect()
    formatted = []
    
    for row in rows:
        formatted.append({
            "year": row["year"],
            "month": row["month"],
            "month_name": get_month_name(row["month"]),
            "week": row["week"],
            "weekly_max_temperature": row["weekly_max_temperature"]
        })
    
    return formatted


def print_hottest_months(results: list):
    """Print formatted hottest months results to console."""
    print("\n" + "=" * 60)
    print("Hottest Months by Average Maximum Temperature")
    print("=" * 60)
    print(f"{'Rank':<6} {'Month':<12} {'Avg Max Temp (°C)':>18}")
    print("-" * 60)
    
    for r in results:
        print(f"{r['rank']:<6} {r['month_name']:<12} {r['avg_max_temperature']:>18.2f}")
    
    print("=" * 60)


def print_weekly_max_temperatures(results: list, limit: int = 50):
    """Print formatted weekly max temperature results to console."""
    print("\n" + "=" * 70)
    print("Weekly Maximum Temperatures for Hottest Months")
    print("=" * 70)
    print(f"{'Year':<6} {'Month':<12} {'Week':>6} {'Max Temp (°C)':>15}")
    print("-" * 70)
    
    for r in results[:limit]:
        print(f"{r['year']:<6} {r['month_name']:<12} {r['week']:>6} {r['weekly_max_temperature']:>15.2f}")
    
    if len(results) > limit:
        print(f"... and {len(results) - limit} more records")
    
    print("=" * 70)


def get_summary_statistics(weekly_max_df: DataFrame) -> dict:
    """
    Calculate summary statistics for weekly max temperatures.
    
    Args:
        weekly_max_df: DataFrame with weekly max temperatures
        
    Returns:
        Dictionary with summary statistics
    """
    from pyspark.sql.functions import min as spark_min
    
    stats = weekly_max_df.agg(
        spark_max("weekly_max_temperature").alias("overall_max"),
        avg("weekly_max_temperature").alias("overall_avg"),
        spark_min("year").alias("min_year"),
        spark_max("year").alias("max_year")
    ).collect()[0]
    
    return {
        "overall_max_temperature": round(stats["overall_max"], 2),
        "overall_avg_temperature": round(stats["overall_avg"], 2),
        "min_year": stats["min_year"],
        "max_year": stats["max_year"],
        "total_weeks": weekly_max_df.count()
    }


# Main execution
if __name__ == "__main__":
    # Get the project root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Define data path
    weather_path = os.path.join(project_root, "dataset", "weatherData.csv")
    
    print("=" * 70)
    print("Sri Lanka Weather Analytics - Weekly Maximum Temperature Analysis")
    print("=" * 70)
    
    # Create Spark session
    print("\n1. Creating Spark session...")
    spark = create_spark_session()
    print(f"   Spark version: {spark.version}")
    
    # Load weather data
    print("\n2. Loading weather data...")
    weather_df = load_weather_data(spark, weather_path)
    print(f"   Total records: {weather_df.count()}")
    
    # Identify hottest months
    print("\n3. Identifying hottest months based on average temperature_2m_max...")
    hottest_months_df = identify_hottest_months(weather_df, top_n=3)
    hottest_months_results = format_hottest_months_results(hottest_months_df)
    print_hottest_months(hottest_months_results)
    
    # Calculate weekly max temperatures for hottest months
    print("\n4. Calculating weekly maximum temperatures for hottest months...")
    weekly_max_df = calculate_weekly_max_temperatures(weather_df, hottest_months_df)
    
    # Get enriched results with month info
    enriched_df = get_weekly_max_with_month_info(weekly_max_df, hottest_months_df)
    
    # Format and display results
    weekly_max_results = format_weekly_max_results(weekly_max_df)
    print_weekly_max_temperatures(weekly_max_results, limit=30)
    
    # Show summary statistics
    print("\n5. Summary Statistics:")
    summary = get_summary_statistics(weekly_max_df)
    print(f"   Year range: {summary['min_year']} - {summary['max_year']}")
    print(f"   Total weeks analyzed: {summary['total_weeks']}")
    print(f"   Overall max temperature: {summary['overall_max_temperature']}°C")
    print(f"   Overall avg weekly max: {summary['overall_avg_temperature']}°C")
    
    # Show raw DataFrame output
    print("\n6. Raw DataFrame output (first 20 rows):")
    weekly_max_df.show(20)
    
    # Show yearly breakdown
    print("\n7. Yearly breakdown of weekly max temperatures:")
    yearly_summary = weekly_max_df.groupBy("year").agg(
        spark_max("weekly_max_temperature").alias("max_temp"),
        spark_round(avg("weekly_max_temperature"), 2).alias("avg_temp")
    ).orderBy("year")
    yearly_summary.show(20)
    
    # Stop Spark session
    spark.stop()
    print("\nAnalysis complete!")
