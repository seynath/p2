package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner for precipitation aggregation.
 * Performs partial sum of precipitation hours at the mapper side to reduce network traffic.
 */
public class PrecipitationCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private final DoubleWritable outputValue = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double partialSum = 0.0;
        
        for (DoubleWritable value : values) {
            partialSum += value.get();
        }

        outputValue.set(partialSum);
        context.write(key, outputValue);
    }
}
