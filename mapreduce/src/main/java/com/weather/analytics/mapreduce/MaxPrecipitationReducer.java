package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for finding the month/year with highest total precipitation.
 * Sums precipitation hours for each month-year key and tracks the global maximum.
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

        // Parse the composite key: month\tyear
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
        // Output the final result only if we found valid data
        if (maxPrecipitation > Double.NEGATIVE_INFINITY) {
            String result = formatOutput(maxMonth, maxYear, maxPrecipitation);
            context.write(new Text(result), NullWritable.get());
        }
    }

    /**
     * Formats the output according to requirement 2.3.
     * Format: "Nth month in YYYY had the highest total precipitation of X hr"
     */
    public static String formatOutput(int month, int year, double precipitation) {
        String monthOrdinal = getOrdinal(month);
        return String.format("%s month in %d had the highest total precipitation of %.2f hr",
                monthOrdinal, year, precipitation);
    }

    /**
     * Converts a month number to its ordinal representation.
     * e.g., 1 -> "1st", 2 -> "2nd", 3 -> "3rd", 4 -> "4th", etc.
     */
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

    // For testing purposes
    public int getMaxMonth() { return maxMonth; }
    public int getMaxYear() { return maxYear; }
    public double getMaxPrecipitation() { return maxPrecipitation; }
    
    // Reset state for testing
    public void reset() {
        maxMonth = 0;
        maxYear = 0;
        maxPrecipitation = Double.NEGATIVE_INFINITY;
    }
}
