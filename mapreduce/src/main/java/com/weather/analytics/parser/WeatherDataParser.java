package com.weather.analytics.parser;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.validation.ValidationResult;
import com.weather.analytics.validation.WeatherDataValidator;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Parser for weatherData.csv records.
 * Expected CSV format: location_id,date,weather_code,temperature_2m_max,...
 */
public class WeatherDataParser {

    private static final int EXPECTED_COLUMNS = 20;

    /**
     * Parses a CSV line into a WeatherRecord.
     * 
     * @param line the CSV line to parse
     * @return Optional containing the parsed record, or empty if parsing fails
     */
    public static Optional<WeatherRecord> parse(String line) {
        if (line == null || line.trim().isEmpty()) {
            return Optional.empty();
        }

        String[] fields = line.split(",", -1);
        if (fields.length < EXPECTED_COLUMNS) {
            return Optional.empty();
        }

        try {
            int locationId = Integer.parseInt(fields[0].trim());
            LocalDate date = DateParser.parse(fields[1].trim());
            int weatherCode = Integer.parseInt(fields[2].trim());

            WeatherRecord record = WeatherRecord.builder()
                    .locationId(locationId)
                    .date(date)
                    .weatherCode(weatherCode)
                    .temperature2mMax(parseDouble(fields[3]))
                    .temperature2mMin(parseDouble(fields[4]))
                    .temperature2mMean(parseDouble(fields[5]))
                    .apparentTemperatureMax(parseDouble(fields[6]))
                    .apparentTemperatureMin(parseDouble(fields[7]))
                    .apparentTemperatureMean(parseDouble(fields[8]))
                    .daylightDuration(parseDouble(fields[9]))
                    .sunshineDuration(parseDouble(fields[10]))
                    .precipitationSum(parseDouble(fields[11]))
                    .rainSum(parseDouble(fields[12]))
                    .precipitationHours(parseDouble(fields[13]))
                    .windSpeed10mMax(parseDouble(fields[14]))
                    .windGusts10mMax(parseDouble(fields[15]))
                    .windDirection10mDominant(parseDouble(fields[16]))
                    .shortwaveRadiationSum(parseDouble(fields[17]))
                    .et0FaoEvapotranspiration(parseDouble(fields[18]))
                    .sunrise(fields[19].trim())
                    .sunset(fields.length > 20 ? fields[20].trim() : "")
                    .build();

            return Optional.of(record);
        } catch (Exception e) {
            return Optional.empty();
        }
    }


    /**
     * Parses a CSV line and validates the resulting record.
     * 
     * @param line the CSV line to parse
     * @return ValidationResult containing the record if valid, or errors if invalid
     */
    public static ValidationResult<WeatherRecord> parseAndValidate(String line) {
        Optional<WeatherRecord> recordOpt = parse(line);
        if (recordOpt.isEmpty()) {
            return ValidationResult.invalid("Failed to parse CSV line");
        }
        return WeatherDataValidator.validate(recordOpt.get());
    }

    /**
     * Checks if a line is the header row.
     * 
     * @param line the line to check
     * @return true if this is the header row
     */
    public static boolean isHeader(String line) {
        return line != null && line.startsWith("location_id,");
    }

    private static Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
