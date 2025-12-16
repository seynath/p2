-- ============================================================================
-- Sri Lanka Weather Analytics - Data Loading Script
-- ============================================================================
-- This script loads CSV data into Hive tables.
-- Run this after create_tables.hql if tables already exist.
-- Requirements: 7.1 (location_id as primary key), 7.4 (schema validation)
-- ============================================================================

-- ============================================================================
-- Load Location Data
-- ============================================================================
-- Load location data from CSV file into location_data table
-- Note: Adjust the path based on your HDFS or local file system setup

LOAD DATA LOCAL INPATH 'dataset/locationData.csv'
OVERWRITE INTO TABLE location_data;

-- Verify location data load
SELECT 'Location data loaded successfully' AS status, COUNT(*) AS records 
FROM location_data;

-- ============================================================================
-- Load Weather Data (Staging)
-- ============================================================================
-- Load raw weather data from CSV into staging table

LOAD DATA LOCAL INPATH 'dataset/weatherData.csv'
OVERWRITE INTO TABLE weather_data_staging;

-- Verify staging data load
SELECT 'Weather staging data loaded successfully' AS status, COUNT(*) AS records 
FROM weather_data_staging;

-- ============================================================================
-- Transform and Load Weather Data into Partitioned Table
-- ============================================================================
-- Enable dynamic partitioning for automatic partition creation
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=500;

-- Insert data from staging to partitioned table with date parsing
-- Date format in source: M/D/YYYY (e.g., 1/1/2010, 12/31/2024)
INSERT OVERWRITE TABLE weather_data PARTITION (year, month)
SELECT
    location_id,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS observation_date,
    DAY(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS day_of_month,
    weather_code,
    temperature_2m_max,
    temperature_2m_min,
    temperature_2m_mean,
    apparent_temperature_max,
    apparent_temperature_min,
    apparent_temperature_mean,
    daylight_duration,
    sunshine_duration,
    precipitation_sum,
    rain_sum,
    precipitation_hours,
    wind_speed_10m_max,
    wind_gusts_10m_max,
    wind_direction_10m_dominant,
    shortwave_radiation_sum,
    et0_fao_evapotranspiration,
    sunrise,
    sunset,
    YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS year,
    MONTH(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS month
FROM weather_data_staging
WHERE date_str IS NOT NULL 
  AND date_str != ''
  AND location_id IS NOT NULL;

-- ============================================================================
-- Data Validation Queries
-- ============================================================================

-- Verify final data load
SELECT 'Weather data loaded successfully' AS status, COUNT(*) AS records 
FROM weather_data;

-- Check for any orphan records (weather data without matching location)
SELECT 'Orphan weather records (no matching location)' AS check_type, COUNT(*) AS count
FROM weather_data w
LEFT JOIN location_data l ON w.location_id = l.location_id
WHERE l.location_id IS NULL;

-- Verify join integrity (Requirement 7.1)
SELECT 'Records with valid location join' AS check_type, COUNT(*) AS count
FROM weather_data w
INNER JOIN location_data l ON w.location_id = l.location_id;

-- Show data distribution by year
SELECT year, COUNT(*) AS record_count
FROM weather_data
GROUP BY year
ORDER BY year;

-- Show data distribution by location
SELECT l.city_name, COUNT(*) AS record_count
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
GROUP BY l.city_name
ORDER BY record_count DESC;
