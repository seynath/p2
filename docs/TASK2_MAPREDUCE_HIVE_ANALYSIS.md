# Task 2: MapReduce and Hive/Pig Analytics

## Sri Lanka Weather Analytics Platform

This document presents the implementation and results of batch processing analytics using Hadoop MapReduce and Apache Hive for analyzing Sri Lanka's historical weather data (2010 - June 2024).

---

## Part A: Hadoop MapReduce Analytics

### Question 1.1: District Monthly Precipitation and Mean Temperature

**Objective:** Calculate the total precipitation hours AND mean temperature (temperature_2m_mean) for each district per month over the past decade.

#### Final Output (Sample)

```
District          Month   Year    Total Precipitation    Mean Temperature
------------------------------------------------------------------------------------------
Colombo           1       2023    113.00 hours           mean_temp=25.18°C
Gampaha           2       2023    117.00 hours           mean_temp=25.79°C
Ratnapura         6       2023    573.00 hours           mean_temp=25.94°C
Kandy             10      2023    328.00 hours           mean_temp=23.41°C
Nuwara Eliya      7       2023    393.00 hours           mean_temp=15.36°C
Polonnaruwa       4       2023    116.00 hours           mean_temp=28.05°C
Jaffna            3       2023    119.00 hours           mean_temp=27.81°C
Trincomalee       5       2023    110.00 hours           mean_temp=28.77°C
...
```

**Key Finding:** Ratnapura has the highest total precipitation across the dataset (62,920 hours), while Jaffna shows the highest mean temperatures during April.

#### Code Listing: PrecipitationMapper.java

```java
package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Mapper for precipitation aggregation.
 * Emits (district-month-year, precipitation_hours) for each weather record.
 * Uses distributed cache to load location data for district name lookup.
 */
public class PrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text outputKey = new Text();
    private final DoubleWritable outputValue = new DoubleWritable();
    private Map<Integer, String> locationIdToDistrict;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        locationIdToDistrict = new HashMap<>();
        
        // Load location data from distributed cache
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            loadLocationData(cacheFiles[0].getPath());
        }
    }

    private void loadLocationData(String path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path))) {
            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] fields = line.split(",", -1);
                if (fields.length >= 8) {
                    try {
                        int locationId = Integer.parseInt(fields[0].trim());
                        String cityName = fields[7].trim();
                        locationIdToDistrict.put(locationId, cityName);
                    } catch (NumberFormatException e) {
                        // Skip invalid records
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        
        // Skip header row
        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(line);
        if (recordOpt.isEmpty()) {
            context.getCounter("Precipitation", "ParseErrors").increment(1);
            return;
        }

        WeatherRecord record = recordOpt.get();
        
        // Skip records with null precipitation hours
        if (record.getPrecipitationHours() == null) {
            context.getCounter("Precipitation", "NullPrecipitation").increment(1);
            return;
        }

        // Get district name from location lookup
        String district = locationIdToDistrict.get(record.getLocationId());
        if (district == null) {
            district = "Unknown-" + record.getLocationId();
        }

        // Create composite key: district-month-year
        String compositeKey = String.format("%s\t%d\t%d", 
                district, record.getMonth(), record.getYear());
        
        outputKey.set(compositeKey);
        outputValue.set(record.getPrecipitationHours());
        
        context.write(outputKey, outputValue);
    }
}
```

#### Code Listing: PrecipitationReducer.java

```java
package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for precipitation aggregation.
 * Sums precipitation hours for each district-month-year key.
 */
public class PrecipitationReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double totalPrecipitationHours = 0.0;
        int count = 0;
        
        for (DoubleWritable value : values) {
            totalPrecipitationHours += value.get();
            count++;
        }

        // Parse the composite key: district\tmonth\tyear
        String[] keyParts = key.toString().split("\t");
        if (keyParts.length < 3) {
            context.getCounter("Precipitation", "InvalidKey").increment(1);
            return;
        }

        String district = keyParts[0];
        int month = Integer.parseInt(keyParts[1]);
        int year = Integer.parseInt(keyParts[2]);

        // Format output
        String result = String.format("%.2f hours (records: %d)", 
                totalPrecipitationHours, count);
        
        String outputKeyStr = String.format("%s\t%d\t%d", district, month, year);
        
        outputValue.set(result);
        context.write(new Text(outputKeyStr), outputValue);
    }
}
```

---

#### Code Listing: TemperatureMapper.java (Mean Temperature Component)

```java
package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Mapper for temperature aggregation.
 * Emits (district-month-year, temperature_2m_mean) for each weather record.
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text outputKey = new Text();
    private final DoubleWritable outputValue = new DoubleWritable();
    private Map<Integer, String> locationIdToDistrict;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        locationIdToDistrict = new HashMap<>();
        
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            loadLocationData(cacheFiles[0].getPath());
        }
    }

    private void loadLocationData(String path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(path))) {
            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] fields = line.split(",", -1);
                if (fields.length >= 8) {
                    try {
                        int locationId = Integer.parseInt(fields[0].trim());
                        String cityName = fields[7].trim();
                        locationIdToDistrict.put(locationId, cityName);
                    } catch (NumberFormatException e) {
                        // Skip invalid records
                    }
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        
        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(line);
        if (recordOpt.isEmpty()) {
            context.getCounter("Temperature", "ParseErrors").increment(1);
            return;
        }

        WeatherRecord record = recordOpt.get();
        
        if (record.getTemperature2mMean() == null) {
            context.getCounter("Temperature", "NullTemperature").increment(1);
            return;
        }

        String district = locationIdToDistrict.get(record.getLocationId());
        if (district == null) {
            district = "Unknown-" + record.getLocationId();
        }

        String compositeKey = String.format("%s\t%d\t%d", 
                district, record.getMonth(), record.getYear());
        
        outputKey.set(compositeKey);
        outputValue.set(record.getTemperature2mMean());
        
        context.write(outputKey, outputValue);
    }
}
```

#### Code Listing: TemperatureReducer.java

```java
package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for temperature aggregation.
 * Calculates mean temperature for each district-month-year key.
 */
public class TemperatureReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double sumTemperature = 0.0;
        int count = 0;
        
        for (DoubleWritable value : values) {
            sumTemperature += value.get();
            count++;
        }

        if (count == 0) {
            context.getCounter("Temperature", "EmptyGroup").increment(1);
            return;
        }

        double meanTemperature = sumTemperature / count;

        String[] keyParts = key.toString().split("\t");
        if (keyParts.length < 3) {
            context.getCounter("Temperature", "InvalidKey").increment(1);
            return;
        }

        String district = keyParts[0];
        int month = Integer.parseInt(keyParts[1]);
        int year = Integer.parseInt(keyParts[2]);

        String result = String.format("mean_temp=%.2f°C (records: %d)", 
                meanTemperature, count);
        
        String outputKeyStr = String.format("%s\t%d\t%d", district, month, year);
        
        outputValue.set(result);
        context.write(new Text(outputKeyStr), outputValue);
    }
}
```

---

### Question 1.2: Highest Precipitation Month/Year Identification

**Objective:** Identify the month and year with the highest total precipitation in the full dataset.

#### Final Output

```
10th month in 2018 had the highest total precipitation of 11007.00 hr
```

**Interpretation:** October 2018 recorded the highest total precipitation hours across all districts in Sri Lanka, coinciding with the inter-monsoon period which typically brings heavy rainfall.

#### Code Listing: MaxPrecipitationMapper.java

```java
package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

/**
 * Mapper for finding the month/year with highest total precipitation.
 * Emits (month-year, precipitation_hours) for each weather record.
 */
public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text outputKey = new Text();
    private final DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        
        if (WeatherDataParser.isHeader(line)) {
            return;
        }

        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(line);
        if (recordOpt.isEmpty()) {
            context.getCounter("MaxPrecipitation", "ParseErrors").increment(1);
            return;
        }

        WeatherRecord record = recordOpt.get();
        
        if (record.getPrecipitationHours() == null) {
            context.getCounter("MaxPrecipitation", "NullPrecipitation").increment(1);
            return;
        }

        // Create composite key: month-year (across all districts)
        String compositeKey = String.format("%d\t%d", 
                record.getMonth(), record.getYear());
        
        outputKey.set(compositeKey);
        outputValue.set(record.getPrecipitationHours());
        
        context.write(outputKey, outputValue);
    }
}
```

#### Code Listing: MaxPrecipitationReducer.java

```java
package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for finding the month/year with highest total precipitation.
 * Output format: "Nth month in YYYY had the highest total precipitation of X hr"
 */
public class MaxPrecipitationReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

    private int maxMonth = 0;
    private int maxYear = 0;
    private double maxPrecipitation = Double.NEGATIVE_INFINITY;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double totalPrecipitationHours = 0.0;
        
        for (DoubleWritable value : values) {
            totalPrecipitationHours += value.get();
        }

        String[] keyParts = key.toString().split("\t");
        if (keyParts.length < 2) {
            context.getCounter("MaxPrecipitation", "InvalidKey").increment(1);
            return;
        }

        int month = Integer.parseInt(keyParts[0]);
        int year = Integer.parseInt(keyParts[1]);

        // Track the maximum
        if (totalPrecipitationHours > maxPrecipitation) {
            maxPrecipitation = totalPrecipitationHours;
            maxMonth = month;
            maxYear = year;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (maxPrecipitation > Double.NEGATIVE_INFINITY) {
            String result = formatOutput(maxMonth, maxYear, maxPrecipitation);
            context.write(new Text(result), NullWritable.get());
        }
    }

    public static String formatOutput(int month, int year, double precipitation) {
        String monthOrdinal = getOrdinal(month);
        return String.format("%s month in %d had the highest total precipitation of %.2f hr",
                monthOrdinal, year, precipitation);
    }

    public static String getOrdinal(int n) {
        if (n >= 11 && n <= 13) {
            return n + "th";
        }
        switch (n % 10) {
            case 1: return n + "st";
            case 2: return n + "nd";
            case 3: return n + "rd";
            default: return n + "th";
        }
    }
}
```

---

## Part B: Apache Hive Analytics

### Question 2.1: Top 10 Most Temperate Cities

**Objective:** Rank the top 10 most temperate cities across the dataset (use temperature_2m_max °C).

#### Final Output

| Rank | City | Max Temperature (°C) |
|------|------|---------------------|
| 1 | Polonnaruwa | 40.3 |
| 2 | Ampara | 39.8 |
| 3 | Moneragala | 39.4 |
| 4 | Vavuniya | 39.4 |
| 5 | Kilinochchi | 39.4 |
| 6 | Batticaloa | 38.9 |
| 7 | Trincomalee | 38.8 |
| 8 | Mullaitivu | 38.6 |
| 9 | Kegalle | 38.4 |
| 10 | Kurunegala | 38.3 |

**Interpretation:** The hottest cities are predominantly in the dry zone (North Central, Eastern, and Northern provinces), with Polonnaruwa recording the highest temperature of 40.3°C.

#### Code Listing: top_temperate_cities.hql

```sql
-- ============================================================================
-- Sri Lanka Weather Analytics - Top 10 Temperate Cities Query
-- ============================================================================
-- Requirements: 2.1 - Rank top 10 cities by maximum temperature
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

-- Alternative: Top 10 Cities with Date of Maximum Temperature
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
```

---

### Question 2.2: Seasonal Evapotranspiration Calculation

**Objective:** Calculate the average evapotranspiration for each major agricultural season in each district over the years:
- **Maha Season** (September to March) - per district per year
- **Yala Season** (April to August) - per district per year

#### Final Output: Maha Season (Sample)

| District | Avg ET0 (mm) |
|----------|-------------|
| Mannar | 4.38 |
| Hambantota | 4.37 |
| Jaffna | 4.31 |
| Mullaitivu | 4.27 |
| Kilinochchi | 4.26 |
| Trincomalee | 4.20 |
| Puttalam | 4.02 |
| Polonnaruwa | 4.01 |
| Matara | 3.96 |
| Vavuniya | 3.96 |
| ... | ... |
| Nuwara Eliya | 3.15 |

#### Final Output: Yala Season (Sample)

| District | Avg ET0 (mm) |
|----------|-------------|
| Polonnaruwa | 5.71 |
| Trincomalee | 5.69 |
| Kilinochchi | 5.66 |
| Mullaitivu | 5.62 |
| Mannar | 5.29 |
| Jaffna | 5.20 |
| Vavuniya | 5.18 |
| Batticaloa | 5.03 |
| Hambantota | 4.95 |
| Ampara | 4.87 |
| ... | ... |
| Nuwara Eliya | 3.23 |

**Interpretation:** Yala season shows higher evapotranspiration rates due to higher temperatures and solar radiation. Polonnaruwa has the highest Yala ET0 (5.71 mm), while Nuwara Eliya has the lowest across both seasons (~3.2 mm).

#### Code Listing: seasonal_evapotranspiration.hql

```sql
-- ============================================================================
-- Sri Lanka Weather Analytics - Seasonal Evapotranspiration Calculation
-- ============================================================================
-- Maha season: September to March (months 9, 10, 11, 12, 1, 2, 3)
-- Yala season: April to August (months 4, 5, 6, 7, 8)
-- Requirements: 2.2
-- ============================================================================

-- Seasonal Evapotranspiration by District and Year
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

-- Summary: Average Evapotranspiration by Season Across All Years
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
```

---

### Hive Table Definitions

#### Code Listing: create_tables.hql

```sql
-- ============================================================================
-- Sri Lanka Weather Analytics - Hive Table Definitions
-- ============================================================================

-- Location Data Table
CREATE TABLE IF NOT EXISTS location_data (
    location_id         INT         COMMENT 'Primary key',
    latitude            DOUBLE      COMMENT 'Geographic latitude',
    longitude           DOUBLE      COMMENT 'Geographic longitude',
    elevation           INT         COMMENT 'Elevation in meters',
    utc_offset_seconds  INT         COMMENT 'UTC offset in seconds',
    timezone            STRING      COMMENT 'Timezone identifier',
    timezone_abbr       STRING      COMMENT 'Timezone abbreviation',
    city_name           STRING      COMMENT 'District/city name'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Weather Data Table (Partitioned by year and month)
CREATE TABLE IF NOT EXISTS weather_data (
    location_id                     INT,
    observation_date                DATE,
    day_of_month                    INT,
    weather_code                    INT,
    temperature_2m_max              DOUBLE,
    temperature_2m_min              DOUBLE,
    temperature_2m_mean             DOUBLE,
    apparent_temperature_max        DOUBLE,
    apparent_temperature_min        DOUBLE,
    apparent_temperature_mean       DOUBLE,
    daylight_duration               DOUBLE,
    sunshine_duration               DOUBLE,
    precipitation_sum               DOUBLE,
    rain_sum                        DOUBLE,
    precipitation_hours             DOUBLE,
    wind_speed_10m_max              DOUBLE,
    wind_gusts_10m_max              DOUBLE,
    wind_direction_10m_dominant     INT,
    shortwave_radiation_sum         DOUBLE,
    et0_fao_evapotranspiration      DOUBLE,
    sunrise                         STRING,
    sunset                          STRING
)
PARTITIONED BY (
    year                            INT,
    month                           INT
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- Load data with dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE weather_data PARTITION (year, month)
SELECT
    location_id,
    TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS observation_date,
    DAY(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS day_of_month,
    weather_code,
    temperature_2m_max,
    temperature_2m_min,
    temperature_2m_mean,
    -- ... other columns ...
    YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS year,
    MONTH(FROM_UNIXTIME(UNIX_TIMESTAMP(date_str, 'M/d/yyyy'))) AS month
FROM weather_data_staging
WHERE date_str IS NOT NULL AND location_id IS NOT NULL;
```

---

## Summary of Results

| Analysis | Key Finding |
|----------|-------------|
| **District Precipitation** | Ratnapura has highest total precipitation (62,920 hours) |
| **Highest Precipitation Month** | October 2018 (11,007 hours) |
| **Hottest City** | Polonnaruwa (40.3°C max) |
| **Highest Maha ET0** | Mannar (4.38 mm) |
| **Highest Yala ET0** | Polonnaruwa (5.71 mm) |
| **Lowest ET0 (both seasons)** | Nuwara Eliya (~3.2 mm) |
