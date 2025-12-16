package com.weather.analytics.parser;

import com.weather.analytics.model.LocationRecord;
import com.weather.analytics.validation.LocationDataValidator;
import com.weather.analytics.validation.ValidationResult;

import java.util.Optional;

/**
 * Parser for locationData.csv records.
 * Expected CSV format: location_id,latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation,city_name
 */
public class LocationDataParser {

    private static final int EXPECTED_COLUMNS = 8;

    /**
     * Parses a CSV line into a LocationRecord.
     * 
     * @param line the CSV line to parse
     * @return Optional containing the parsed record, or empty if parsing fails
     */
    public static Optional<LocationRecord> parse(String line) {
        if (line == null || line.trim().isEmpty()) {
            return Optional.empty();
        }

        String[] fields = line.split(",", -1);
        if (fields.length < EXPECTED_COLUMNS) {
            return Optional.empty();
        }

        try {
            int locationId = Integer.parseInt(fields[0].trim());
            double latitude = Double.parseDouble(fields[1].trim());
            double longitude = Double.parseDouble(fields[2].trim());
            int elevation = Integer.parseInt(fields[3].trim());
            int utcOffsetSeconds = Integer.parseInt(fields[4].trim());
            String timezone = fields[5].trim();
            String timezoneAbbreviation = fields[6].trim();
            String cityName = fields[7].trim();

            return Optional.of(new LocationRecord(
                    locationId, latitude, longitude, elevation,
                    utcOffsetSeconds, timezone, timezoneAbbreviation, cityName
            ));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Parses a CSV line and validates the resulting record.
     * 
     * @param line the CSV line to parse
     * @return ValidationResult containing the record if valid, or errors if invalid
     */
    public static ValidationResult<LocationRecord> parseAndValidate(String line) {
        Optional<LocationRecord> recordOpt = parse(line);
        if (recordOpt.isEmpty()) {
            return ValidationResult.invalid("Failed to parse CSV line");
        }
        return LocationDataValidator.validate(recordOpt.get());
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
}
