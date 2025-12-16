package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for precipitation aggregation.
 * Sums precipitation hours for each district-month-year key.
 * Output format: "District had a total precipitation of X hours with a mean temperature of Y for Nth month"
 * Note: Mean temperature is added by a separate job or combiner; this reducer focuses on precipitation sum.
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

        // Format output according to requirement 2.4
        String result = String.format("%.2f hours (records: %d)", totalPrecipitationHours, count);
        
        // Create a more readable key for output
        String outputKeyStr = String.format("%s\t%d\t%d", district, month, year);
        
        outputValue.set(result);
        context.write(new Text(outputKeyStr), outputValue);
    }
}
