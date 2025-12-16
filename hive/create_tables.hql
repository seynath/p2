-- ============================================================================
-- Sri Lanka Weather Analytics - Hive Table Definitions
-- ============================================================================
-- This script creates the Hive tables for weather and location data analysis.
-- Requirements: 7.1 (location_id as primary key), 7.4 (schema validation)
-- ============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS weather_data;
DROP TABLE IF EXISTS location_data;
DROP TABLE IF EXISTS weather_data_staging;

-- ============================================================================
-- Location Data Table
-- ============================================================================
-- Contains geographic information for 27 Sri Lankan districts/cities
-- Primary key: location_id (used for joining with weather_data)

CREATE TABLE IF NOT EXISTS location_data (
    location_id         INT         COMMENT 'Primary key - unique identifier for each location',
    latitude            DOUBLE      COMMENT 'Geographic latitude coordinate',
    longitude           DOUBLE      COMMENT 'Geographic longitude coordinate',
    elevation           INT         COMMENT 'Elevation in meters above sea level',
    utc_offset_seconds  INT         COMMENT 'UTC offset in seconds',
    timezone            STRING      COMMENT 'Timezone identifier (e.g., Asia/Colombo)',
    timezone_abbr       STRING      COMMENT 'Timezone abbreviation',
    city_name           STRING      COMMENT 'District/city name'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- ============================================================================
-- Weather Data Staging Table (for initial load)
-- ============================================================================
-- Staging table to load raw CSV data before transforming to partitioned table

CREATE TABLE IF NOT EXISTS weather_data_staging (
    location_id                     INT         COMMENT 'Foreign key to location_data',
    date_str                        STRING      COMMENT 'Date in M/D/YYYY format',
    weather_code                    INT         COMMENT 'WMO weather code',
    temperature_2m_max              DOUBLE      COMMENT 'Maximum temperature at 2m (°C)',
    temperature_2m_min              DOUBLE      COMMENT 'Minimum temperature at 2m (°C)',
    temperature_2m_mean             DOUBLE      COMMENT 'Mean temperature at 2m (°C)',
    apparent_temperature_max        DOUBLE      COMMENT 'Maximum apparent temperature (°C)',
    apparent_temperature_min        DOUBLE      COMMENT 'Minimum apparent temperature (°C)',
    apparent_temperature_mean       DOUBLE      COMMENT 'Mean apparent temperature (°C)',
    daylight_duration               DOUBLE      COMMENT 'Daylight duration in seconds',
    sunshine_duration               DOUBLE      COMMENT 'Sunshine duration in seconds',
    precipitation_sum               DOUBLE      COMMENT 'Total precipitation (mm)',
    rain_sum                        DOUBLE      COMMENT 'Total rain (mm)',
    precipitation_hours             DOUBLE      COMMENT 'Hours of precipitation',
    wind_speed_10m_max              DOUBLE      COMMENT 'Maximum wind speed at 10m (km/h)',
    wind_gusts_10m_max              DOUBLE      COMMENT 'Maximum wind gusts at 10m (km/h)',
    wind_direction_10m_dominant     INT         COMMENT 'Dominant wind direction at 10m (degrees)',
    shortwave_radiation_sum         DOUBLE      COMMENT 'Total shortwave radiation (MJ/m²)',
    et0_fao_evapotranspiration      DOUBLE      COMMENT 'FAO reference evapotranspiration (mm)',
    sunrise                         STRING      COMMENT 'Sunrise time (HH:MM)',
    sunset                          STRING      COMMENT 'Sunset time (HH:MM)'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- ============================================================================
-- Weather Data Table (Partitioned)
-- ============================================================================
-- Main weather data table partitioned by year and month for efficient queries
-- Partitioning strategy: year/month enables efficient filtering for:
--   - Monthly aggregations (Requirements 2.1, 2.2)
--   - Seasonal analysis (Requirements 3.2, 3.3)
--   - Year-over-year comparisons

CREATE TABLE IF NOT EXISTS weather_data (
    location_id                     INT         COMMENT 'Foreign key to location_data',
    observation_date                DATE        COMMENT 'Observation date',
    day_of_month                    INT         COMMENT 'Day of the month (1-31)',
    weather_code                    INT         COMMENT 'WMO weather code',
    temperature_2m_max              DOUBLE      COMMENT 'Maximum temperature at 2m (°C)',
    temperature_2m_min              DOUBLE      COMMENT 'Minimum temperature at 2m (°C)',
    temperature_2m_mean             DOUBLE      COMMENT 'Mean temperature at 2m (°C)',
    apparent_temperature_max        DOUBLE      COMMENT 'Maximum apparent temperature (°C)',
    apparent_temperature_min        DOUBLE      COMMENT 'Minimum apparent temperature (°C)',
    apparent_temperature_mean       DOUBLE      COMMENT 'Mean apparent temperature (°C)',
    daylight_duration               DOUBLE      COMMENT 'Daylight duration in seconds',
    sunshine_duration               DOUBLE      COMMENT 'Sunshine duration in seconds',
    precipitation_sum               DOUBLE      COMMENT 'Total precipitation (mm)',
    rain_sum                        DOUBLE      COMMENT 'Total rain (mm)',
    precipitation_hours             DOUBLE      COMMENT 'Hours of precipitation',
    wind_speed_10m_max              DOUBLE      COMMENT 'Maximum wind speed at 10m (km/h)',
    wind_gusts_10m_max              DOUBLE      COMMENT 'Maximum wind gusts at 10m (km/h)',
    wind_direction_10m_dominant     INT         COMMENT 'Dominant wind direction at 10m (degrees)',
    shortwave_radiation_sum         DOUBLE      COMMENT 'Total shortwave radiation (MJ/m²)',
    et0_fao_evapotranspiration      DOUBLE      COMMENT 'FAO reference evapotranspiration (mm)',
    sunrise                         STRING      COMMENT 'Sunrise time (HH:MM)',
    sunset                          STRING      COMMENT 'Sunset time (HH:MM)'
)
PARTITIONED BY (
    year                            INT         COMMENT 'Year of observation (2010-2024)',
    month                           INT         COMMENT 'Month of observation (1-12)'
)
STORED AS ORC
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'transactional'='false'
);

-- ============================================================================
-- Load Location Data
-- ============================================================================
-- Load location data from CSV file into location_data table

LOAD DATA LOCAL INPATH 'dataset/locationData.csv'
OVERWRITE INTO TABLE location_data;

-- ============================================================================
-- Load Weather Data (Staging)
-- ============================================================================
-- Load raw weather data from CSV into staging table

LOAD DATA LOCAL INPATH 'dataset/weatherData.csv'
OVERWRITE INTO TABLE weather_data_staging;

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
-- Verify Data Load
-- ============================================================================
-- Display record counts to verify successful data loading

SELECT 'location_data' AS table_name, COUNT(*) AS record_count FROM location_data
UNION ALL
SELECT 'weather_data' AS table_name, COUNT(*) AS record_count FROM weather_data;

-- Show partition information
SHOW PARTITIONS weather_data;

-- Sample data verification
SELECT 
    w.location_id,
    l.city_name,
    w.observation_date,
    w.temperature_2m_mean,
    w.precipitation_hours,
    w.year,
    w.month
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
LIMIT 10;
