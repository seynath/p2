package com.weather.analytics.validation;

import com.weather.analytics.model.LocationRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates LocationRecord objects for schema compliance.
 * Implements validation rules per Requirements 7.4.
 */
public class LocationDataValidator {

    // Sri Lanka geographic bounds
    private static final double MIN_LATITUDE = 5.9;
    private static final double MAX_LATITUDE = 9.9;
    private static final double MIN_LONGITUDE = 79.5;
    private static final double MAX_LONGITUDE = 82.0;

    // Valid location ID range
    private static final int MIN_LOCATION_ID = 0;
    private static final int MAX_LOCATION_ID = 18;

    /**
     * Validates a LocationRecord for schema compliance.
     * 
     * @param record the record to validate
     * @return ValidationResult containing the record if valid, or errors if invalid
     */
    public static ValidationResult<LocationRecord> validate(LocationRecord record) {
        List<String> errors = new ArrayList<>();

        // Location ID validation
        if (record.getLocationId() < MIN_LOCATION_ID || record.getLocationId() > MAX_LOCATION_ID) {
            errors.add("Location ID must be between " + MIN_LOCATION_ID + " and " + MAX_LOCATION_ID);
        }

        // Latitude validation (Sri Lanka bounds)
        if (record.getLatitude() < MIN_LATITUDE || record.getLatitude() > MAX_LATITUDE) {
            errors.add("Latitude out of Sri Lanka bounds: " + record.getLatitude());
        }

        // Longitude validation (Sri Lanka bounds)
        if (record.getLongitude() < MIN_LONGITUDE || record.getLongitude() > MAX_LONGITUDE) {
            errors.add("Longitude out of Sri Lanka bounds: " + record.getLongitude());
        }

        // City name validation
        if (record.getCityName() == null || record.getCityName().trim().isEmpty()) {
            errors.add("City name is required");
        }

        // Timezone validation
        if (record.getTimezone() == null || record.getTimezone().trim().isEmpty()) {
            errors.add("Timezone is required");
        }

        if (errors.isEmpty()) {
            return ValidationResult.valid(record);
        } else {
            return ValidationResult.invalid(errors);
        }
    }

    /**
     * Checks if a location ID is valid.
     * 
     * @param locationId the location ID to check
     * @return true if valid
     */
    public static boolean isValidLocationId(int locationId) {
        return locationId >= MIN_LOCATION_ID && locationId <= MAX_LOCATION_ID;
    }
}
