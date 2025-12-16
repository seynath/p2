package com.weather.analytics.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Driver class for Weather Analytics MapReduce jobs.
 * Configures and runs precipitation aggregation job.
 */
public class WeatherAnalyticsDriver extends Configured implements Tool {

    public static final String JOB_PRECIPITATION = "precipitation";
    public static final String JOB_TEMPERATURE = "temperature";
    public static final String JOB_COMBINED = "combined";
    public static final String JOB_MAX_PRECIPITATION = "max-precipitation";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: WeatherAnalyticsDriver <job-type> <weather-input> <location-file> <output>");
            System.err.println("  job-type: precipitation | temperature | combined | max-precipitation");
            return 1;
        }

        String jobType = args[0];
        String weatherInput = args[1];
        String locationFile = args[2];
        String output = args[3];

        switch (jobType.toLowerCase()) {
            case JOB_PRECIPITATION:
                return runPrecipitationJob(weatherInput, locationFile, output);
            case JOB_TEMPERATURE:
                return runTemperatureJob(weatherInput, locationFile, output);
            case JOB_COMBINED:
                return runCombinedJob(weatherInput, locationFile, output);
            case JOB_MAX_PRECIPITATION:
                return runMaxPrecipitationJob(weatherInput, output);
            default:
                System.err.println("Unknown job type: " + jobType);
                return 1;
        }
    }

    /**
     * Runs the temperature aggregation job.
     * Calculates mean temperature for each district per month.
     */
    public int runTemperatureJob(String weatherInput, String locationFile, String output) 
            throws Exception {
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "District Monthly Mean Temperature Calculation");
        job.setJarByClass(WeatherAnalyticsDriver.class);

        // Set mapper and reducer (no combiner for mean calculation to preserve count)
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        // Set mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(weatherInput));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // Add location file to distributed cache for join
        job.addCacheFile(new URI(locationFile));

        // Run the job
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * Runs the combined precipitation and temperature aggregation job.
     * Calculates total precipitation hours and mean temperature for each district per month.
     * Output format: "District had a total precipitation of X hours with a mean temperature of Y for Nth month"
     */
    public int runCombinedJob(String weatherInput, String locationFile, String output) 
            throws Exception {
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "District Monthly Precipitation and Temperature Summary");
        job.setJarByClass(WeatherAnalyticsDriver.class);

        // Set mapper and reducer
        job.setMapperClass(CombinedWeatherMapper.class);
        job.setReducerClass(CombinedWeatherReducer.class);

        // Set mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(weatherInput));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // Add location file to distributed cache for join
        job.addCacheFile(new URI(locationFile));

        // Run the job
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * Runs the precipitation aggregation job.
     * Calculates total precipitation hours for each district per month.
     */
    public int runPrecipitationJob(String weatherInput, String locationFile, String output) 
            throws Exception {
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "District Monthly Precipitation Aggregation");
        job.setJarByClass(WeatherAnalyticsDriver.class);

        // Set mapper, combiner, and reducer
        job.setMapperClass(PrecipitationMapper.class);
        job.setCombinerClass(PrecipitationCombiner.class);
        job.setReducerClass(PrecipitationReducer.class);

        // Set mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(weatherInput));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // Add location file to distributed cache for join
        job.addCacheFile(new URI(locationFile));

        // Run the job
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * Runs the max precipitation job.
     * Finds the month/year with the highest total precipitation across all districts.
     * Output format: "Nth month in YYYY had the highest total precipitation of X hr"
     */
    public int runMaxPrecipitationJob(String weatherInput, String output) 
            throws Exception {
        Configuration conf = getConf();
        
        Job job = Job.getInstance(conf, "Find Month/Year with Highest Total Precipitation");
        job.setJarByClass(WeatherAnalyticsDriver.class);

        // Set mapper and reducer
        job.setMapperClass(MaxPrecipitationMapper.class);
        job.setReducerClass(MaxPrecipitationReducer.class);

        // Use a single reducer to find global maximum
        job.setNumReduceTasks(1);

        // Set mapper output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Set final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.NullWritable.class);

        // Set input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(weatherInput));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // Run the job
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WeatherAnalyticsDriver(), args);
        System.exit(exitCode);
    }
}
