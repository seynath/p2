package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner for temperature aggregation.
 * Note: For mean calculation, we cannot simply combine values as we need both sum and count.
 * This combiner passes through individual values to preserve count information.
 * An alternative approach would be to use a custom Writable that tracks sum and count.
 * 
 * For simplicity, this combiner does not aggregate values, allowing the reducer
 * to receive all individual temperature readings for accurate mean calculation.
 */
public class TemperatureCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        // Pass through each value individually to preserve count for mean calculation
        // This is intentional - combining would lose count information needed for accurate mean
        for (DoubleWritable value : values) {
            outputValue.set(value.get());
            context.write(key, outputValue);
        }
    }
}
