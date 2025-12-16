package com.weather.analytics.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combined reducer for precipitation and temperature aggregation.
 * Calculates total precipitation hours and mean temperature for each district-month-year key.
 * Output format: "District had a total precipitation of X hours with a mean temperature of Y for Nth month"
 */
public class CombinedWeatherReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        double totalPrecipitationHours = 0.0;
        int precipitationCount = 0;
        double sumTemperature = 0.0;
        int temperatureCount = 0;
        
        for (Text value : values) {
            String valueStr = value.toString();
            
            if (valueStr.startsWith(CombinedWeatherMapper.PRECIPITATION_PREFIX)) {
                String precipStr = valueStr.substring(CombinedWeatherMapper.PRECIPITATION_PREFIX.length());
                try {
                    totalPrecipitationHours += Double.parseDouble(precipStr);
                    precipitationCount++;
                } catch (NumberFormatException e) {
                    context.getCounter("CombinedWeather", "InvalidPrecipitation").increment(1);
                }
            } else if (valueStr.startsWith(CombinedWeatherMapper.TEMPERATURE_PREFIX)) {
                String tempStr = valueStr.substring(CombinedWeatherMapper.TEMPERATURE_PREFIX.length());
                try {
                    sumTemperature += Double.parseDouble(tempStr);
                    temperatureCount++;
                } catch (NumberFormatException e) {
                    context.getCounter("CombinedWeather", "InvalidTemperature").increment(1);
                }
            }
        }

        // Parse the composite key: district\tmonth\tyear
        String[] keyParts = key.toString().split("\t");
        if (keyParts.length < 3) {
            context.getCounter("CombinedWeather", "InvalidKey").increment(1);
            return;
        }

        String district = keyParts[0];
        int month = Integer.parseInt(keyParts[1]);
        int year = Integer.parseInt(keyParts[2]);

        // Calculate mean temperature
        double meanTemperature = temperatureCount > 0 ? sumTemperature / temperatureCount : 0.0;

        // Format output according to requirement 2.4
        // "District had a total precipitation of X hours with a mean temperature of Y for Nth month"
        String monthOrdinal = getMonthOrdinal(month);
        String result = String.format("%s had a total precipitation of %.2f hours with a mean temperature of %.2f°C for %s month in %d",
                district, totalPrecipitationHours, meanTemperature, monthOrdinal, year);
        
        outputValue.set(result);
        context.write(new Text(""), outputValue);
    }

    /**
     * Converts a month number to its ordinal representation.
     * 
     * @param month the month number (1-12)
     * @return the ordinal string (e.g., "1st", "2nd", "3rd", "4th")
     */
    public static String getMonthOrdinal(int month) {
        if (month < 1 || month > 12) {
            return month + "th";
        }
        
        String[] suffixes = {"th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th"};
        if (month == 11 || month == 12) {
            return month + "th";
        }
        return month + suffixes[month % 10];
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
