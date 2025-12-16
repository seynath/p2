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
 * Aggregates across all districts to find global maximum.
 */
public class MaxPrecipitationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text outputKey = new Text();
    private final DoubleWritable outputValue = new DoubleWritable();

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
            context.getCounter("MaxPrecipitation", "ParseErrors").increment(1);
            return;
        }

        WeatherRecord record = recordOpt.get();
        
        // Skip records with null precipitation hours
        if (record.getPrecipitationHours() == null) {
            context.getCounter("MaxPrecipitation", "NullPrecipitation").increment(1);
            return;
        }

        // Create composite key: month-year (across all districts)
        String compositeKey = String.format("%d\t%d", record.getMonth(), record.getYear());
        
        outputKey.set(compositeKey);
        outputValue.set(record.getPrecipitationHours());
        
        context.write(outputKey, outputValue);
    }
}
