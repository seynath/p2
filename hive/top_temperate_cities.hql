-- ============================================================================
-- Sri Lanka Weather Analytics - Top 10 Temperate Cities Query
-- ============================================================================
-- This script ranks the top 10 most temperate cities based on temperature_2m_max
-- Requirements: 3.1 - Rank top 10 cities by maximum temperature
-- ============================================================================

-- ============================================================================
-- Top 10 Most Temperate Cities by Maximum Temperature
-- ============================================================================
-- This query finds the top 10 cities with the highest recorded maximum 
-- temperature (temperature_2m_max) across the entire dataset.
-- 
-- The query:
-- 1. Joins weather_data with location_data using location_id
-- 2. Finds the maximum temperature_2m_max for each city
-- 3. Ranks cities by their maximum temperature in descending order
-- 4. Returns the top 10 cities with their maximum temperatures
-- ============================================================================

SELECT 
    l.city_name,
    MAX(w.temperature_2m_max) AS max_temperature_celsius,
    COUNT(*) AS total_observations
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.temperature_2m_max IS NOT NULL
GROUP BY l.city_name
ORDER BY max_temperature_celsius DESC
LIMIT 10;

-- ============================================================================
-- Alternative: Top 10 Cities with Date of Maximum Temperature
-- ============================================================================
-- This query also shows when the maximum temperature was recorded for each city

SELECT 
    ranked.city_name,
    ranked.max_temperature_celsius,
    ranked.observation_date AS date_of_max_temp
FROM (
    SELECT 
        l.city_name,
        w.temperature_2m_max AS max_temperature_celsius,
        w.observation_date,
        ROW_NUMBER() OVER (
            PARTITION BY l.city_name 
            ORDER BY w.temperature_2m_max DESC
        ) AS temp_rank
    FROM weather_data w
    JOIN location_data l ON w.location_id = l.location_id
    WHERE w.temperature_2m_max IS NOT NULL
) ranked
WHERE ranked.temp_rank = 1
ORDER BY ranked.max_temperature_celsius DESC
LIMIT 10;
