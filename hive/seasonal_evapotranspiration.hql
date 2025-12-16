-- ============================================================================
-- Sri Lanka Weather Analytics - Seasonal Evapotranspiration Calculation
-- ============================================================================
-- This script calculates average evapotranspiration for agricultural seasons:
-- - Maha season: September to March (months 9, 10, 11, 12, 1, 2, 3)
-- - Yala season: April to August (months 4, 5, 6, 7, 8)
-- 
-- Requirements: 3.2 - Maha season average ET0 per district per year
--               3.3 - Yala season average ET0 per district per year
-- ============================================================================

-- ============================================================================
-- Seasonal Evapotranspiration by District and Year
-- ============================================================================
-- This query calculates the average FAO reference evapotranspiration (ET0)
-- for each agricultural season, grouped by district and year.
--
-- Note on Maha season year assignment:
-- Maha season spans September to March, crossing calendar years.
-- We assign the Maha season to the year in which it ENDS (Jan-Mar).
-- For example: Sep 2010 - Mar 2011 is assigned to Maha 2011.
-- This means Sep-Dec records use (year + 1) as the season year.
-- ============================================================================

SELECT 
    l.city_name AS district,
    CASE 
        WHEN w.month IN (9, 10, 11, 12) THEN w.year + 1
        ELSE w.year
    END AS season_year,
    'Maha' AS season,
    ROUND(AVG(w.et0_fao_evapotranspiration), 4) AS avg_evapotranspiration_mm,
    COUNT(*) AS observation_count
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.month IN (9, 10, 11, 12, 1, 2, 3)
  AND w.et0_fao_evapotranspiration IS NOT NULL
GROUP BY 
    l.city_name,
    CASE 
        WHEN w.month IN (9, 10, 11, 12) THEN w.year + 1
        ELSE w.year
    END

UNION ALL

SELECT 
    l.city_name AS district,
    w.year AS season_year,
    'Yala' AS season,
    ROUND(AVG(w.et0_fao_evapotranspiration), 4) AS avg_evapotranspiration_mm,
    COUNT(*) AS observation_count
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.month IN (4, 5, 6, 7, 8)
  AND w.et0_fao_evapotranspiration IS NOT NULL
GROUP BY 
    l.city_name,
    w.year

ORDER BY district, season_year, season;

-- ============================================================================
-- Alternative: Detailed View with Season Date Ranges
-- ============================================================================
-- This query provides additional context including the date range for each
-- season calculation.

SELECT 
    l.city_name AS district,
    CASE 
        WHEN w.month IN (9, 10, 11, 12) THEN w.year + 1
        ELSE w.year
    END AS season_year,
    'Maha' AS season,
    CONCAT(
        CASE WHEN w.month IN (9, 10, 11, 12) THEN CAST(w.year AS STRING) ELSE CAST(w.year - 1 AS STRING) END,
        '-Sep to ',
        CASE WHEN w.month IN (9, 10, 11, 12) THEN CAST(w.year + 1 AS STRING) ELSE CAST(w.year AS STRING) END,
        '-Mar'
    ) AS season_period,
    ROUND(AVG(w.et0_fao_evapotranspiration), 4) AS avg_evapotranspiration_mm,
    ROUND(MIN(w.et0_fao_evapotranspiration), 4) AS min_evapotranspiration_mm,
    ROUND(MAX(w.et0_fao_evapotranspiration), 4) AS max_evapotranspiration_mm,
    COUNT(*) AS observation_count
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.month IN (9, 10, 11, 12, 1, 2, 3)
  AND w.et0_fao_evapotranspiration IS NOT NULL
GROUP BY 
    l.city_name,
    CASE 
        WHEN w.month IN (9, 10, 11, 12) THEN w.year + 1
        ELSE w.year
    END,
    CONCAT(
        CASE WHEN w.month IN (9, 10, 11, 12) THEN CAST(w.year AS STRING) ELSE CAST(w.year - 1 AS STRING) END,
        '-Sep to ',
        CASE WHEN w.month IN (9, 10, 11, 12) THEN CAST(w.year + 1 AS STRING) ELSE CAST(w.year AS STRING) END,
        '-Mar'
    )

UNION ALL

SELECT 
    l.city_name AS district,
    w.year AS season_year,
    'Yala' AS season,
    CONCAT(CAST(w.year AS STRING), '-Apr to ', CAST(w.year AS STRING), '-Aug') AS season_period,
    ROUND(AVG(w.et0_fao_evapotranspiration), 4) AS avg_evapotranspiration_mm,
    ROUND(MIN(w.et0_fao_evapotranspiration), 4) AS min_evapotranspiration_mm,
    ROUND(MAX(w.et0_fao_evapotranspiration), 4) AS max_evapotranspiration_mm,
    COUNT(*) AS observation_count
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.month IN (4, 5, 6, 7, 8)
  AND w.et0_fao_evapotranspiration IS NOT NULL
GROUP BY 
    l.city_name,
    w.year,
    CONCAT(CAST(w.year AS STRING), '-Apr to ', CAST(w.year AS STRING), '-Aug')

ORDER BY district, season_year, season;

-- ============================================================================
-- Summary: Average Evapotranspiration by Season Across All Years
-- ============================================================================
-- This query provides a high-level summary comparing Maha vs Yala seasons
-- across all districts.

SELECT 
    l.city_name AS district,
    'Maha' AS season,
    ROUND(AVG(w.et0_fao_evapotranspiration), 4) AS avg_evapotranspiration_mm,
    COUNT(*) AS total_observations
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.month IN (9, 10, 11, 12, 1, 2, 3)
  AND w.et0_fao_evapotranspiration IS NOT NULL
GROUP BY l.city_name

UNION ALL

SELECT 
    l.city_name AS district,
    'Yala' AS season,
    ROUND(AVG(w.et0_fao_evapotranspiration), 4) AS avg_evapotranspiration_mm,
    COUNT(*) AS total_observations
FROM weather_data w
JOIN location_data l ON w.location_id = l.location_id
WHERE w.month IN (4, 5, 6, 7, 8)
  AND w.et0_fao_evapotranspiration IS NOT NULL
GROUP BY l.city_name

ORDER BY district, season;
