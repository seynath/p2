package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for temperature aggregation.
 * Calculates mean temperature for each district-month-year key.
 * Output format: "District had a total precipitation of X hours with a mean temperature of Y for Nth month"
 * Note: This reducer outputs mean temperature; final combined output is produced by a separate job.
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

        // Avoid division by zero
        if (count == 0) {
            context.getCounter("Temperature", "EmptyGroup").increment(1);
            return;
        }

        double meanTemperature = sumTemperature / count;

        // Parse the composite key: district\tmonth\tyear
        String[] keyParts = key.toString().split("\t");
        if (keyParts.length < 3) {
            context.getCounter("Temperature", "InvalidKey").increment(1);
            return;
        }

        String district = keyParts[0];
        int month = Integer.parseInt(keyParts[1]);
        int year = Integer.parseInt(keyParts[2]);

        // Format output according to requirement 2.4
        // Note: Full format "District had a total precipitation of X hours with a mean temperature of Y for Nth month"
        // requires combining with precipitation data. This reducer outputs temperature portion.
        String result = String.format("mean_temp=%.2f°C (records: %d)", meanTemperature, count);
        
        // Create a more readable key for output
        String outputKeyStr = String.format("%s\t%d\t%d", district, month, year);
        
        outputValue.set(result);
        context.write(new Text(outputKeyStr), outputValue);
    }

    /**
     * Calculates mean temperature from a sum and count.
     * Exposed for testing purposes.
     * 
     * @param sum the sum of temperature values
     * @param count the number of values
     * @return the mean temperature, or 0.0 if count is 0
     */
    public static double calculateMean(double sum, int count) {
        if (count == 0) {
            return 0.0;
        }
        return sum / count;
    }
}
