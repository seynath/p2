package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
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
            context.getCounter("Precipitation", "UnknownLocation").increment(1);
        }

        // Create composite key: district-month-year
        String compositeKey = String.format("%s\t%d\t%d", 
                district, record.getMonth(), record.getYear());
        
        outputKey.set(compositeKey);
        outputValue.set(record.getPrecipitationHours());
        
        context.write(outputKey, outputValue);
    }
}
