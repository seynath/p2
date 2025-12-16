package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
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
 * Combined mapper for precipitation and temperature aggregation.
 * Emits (district-month-year, "P:precipitation_hours" or "T:temperature_2m_mean") for each weather record.
 * Uses distributed cache to load location data for district name lookup.
 * 
 * Output format uses prefixes to distinguish data types:
 * - "P:" prefix for precipitation hours
 * - "T:" prefix for temperature mean
 */
public class CombinedWeatherMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
    private Map<Integer, String> locationIdToDistrict;

    public static final String PRECIPITATION_PREFIX = "P:";
    public static final String TEMPERATURE_PREFIX = "T:";

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

    /**
     * Loads location data from a map (for testing purposes).
     */
    public void setLocationData(Map<Integer, String> locationData) {
        this.locationIdToDistrict = locationData;
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
            context.getCounter("CombinedWeather", "ParseErrors").increment(1);
            return;
        }

        WeatherRecord record = recordOpt.get();
        
        // Get district name from location lookup
        String district = locationIdToDistrict.get(record.getLocationId());
        if (district == null) {
            district = "Unknown-" + record.getLocationId();
            context.getCounter("CombinedWeather", "UnknownLocation").increment(1);
        }

        // Create composite key: district-month-year
        String compositeKey = String.format("%s\t%d\t%d", 
                district, record.getMonth(), record.getYear());
        outputKey.set(compositeKey);

        // Emit precipitation hours if available
        if (record.getPrecipitationHours() != null) {
            outputValue.set(PRECIPITATION_PREFIX + record.getPrecipitationHours());
            context.write(outputKey, outputValue);
        } else {
            context.getCounter("CombinedWeather", "NullPrecipitation").increment(1);
        }

        // Emit temperature mean if available
        if (record.getTemperature2mMean() != null) {
            outputValue.set(TEMPERATURE_PREFIX + record.getTemperature2mMean());
            context.write(outputKey, outputValue);
        } else {
            context.getCounter("CombinedWeather", "NullTemperature").increment(1);
        }
    }
}
