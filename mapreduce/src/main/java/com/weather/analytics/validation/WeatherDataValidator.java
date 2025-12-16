package com.weather.analytics.validation;

import com.weather.analytics.model.WeatherRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates WeatherRecord objects for schema compliance.
 * Implements validation rules per Requirements 7.4.
 */
public class WeatherDataValidator {

    // Temperature range for Sri Lanka (tropical climate)
    private static final double MIN_TEMPERATURE = -10.0;
    private static final double MAX_TEMPERATURE = 50.0;

    // Valid location ID range (0-18 for Sri Lanka districts)
    private static final int MIN_LOCATION_ID = 0;
    private static final int MAX_LOCATION_ID = 18;

    /**
     * Validates a WeatherRecord for schema compliance.
     * 
     * @param record the record to validate
     * @return ValidationResult containing the record if valid, or errors if invalid
     */
    public static ValidationResult<WeatherRecord> validate(WeatherRecord record) {
        List<String> errors = new ArrayList<>();

        // Required field validation
        if (record.getDate() == null) {
            errors.add("Date is required");
        }

        // Location ID validation
        if (record.getLocationId() < MIN_LOCATION_ID || record.getLocationId() > MAX_LOCATION_ID) {
            errors.add("Location ID must be between " + MIN_LOCATION_ID + " and " + MAX_LOCATION_ID);
        }

        // Temperature range validation (flag for review, not reject)
        if (record.getTemperature2mMax() != null) {
            if (record.getTemperature2mMax() < MIN_TEMPERATURE || record.getTemperature2mMax() > MAX_TEMPERATURE) {
                errors.add("Temperature max out of range: " + record.getTemperature2mMax());
            }
        }

        if (record.getTemperature2mMin() != null) {
            if (record.getTemperature2mMin() < MIN_TEMPERATURE || record.getTemperature2mMin() > MAX_TEMPERATURE) {
                errors.add("Temperature min out of range: " + record.getTemperature2mMin());
            }
        }

        if (record.getTemperature2mMean() != null) {
            if (record.getTemperature2mMean() < MIN_TEMPERATURE || record.getTemperature2mMean() > MAX_TEMPERATURE) {
                errors.add("Temperature mean out of range: " + record.getTemperature2mMean());
            }
        }

        // Temperature consistency check
        if (record.getTemperature2mMax() != null && record.getTemperature2mMin() != null) {
            if (record.getTemperature2mMax() < record.getTemperature2mMin()) {
                errors.add("Max temperature cannot be less than min temperature");
            }
        }

        // Precipitation hours validation (non-negative)
        if (record.getPrecipitationHours() != null && record.getPrecipitationHours() < 0) {
            errors.add("Precipitation hours cannot be negative");
        }

        // Shortwave radiation validation (non-negative)
        if (record.getShortwaveRadiationSum() != null && record.getShortwaveRadiationSum() < 0) {
            errors.add("Shortwave radiation cannot be negative");
        }

        // Evapotranspiration validation (non-negative)
        if (record.getEt0FaoEvapotranspiration() != null && record.getEt0FaoEvapotranspiration() < 0) {
            errors.add("Evapotranspiration cannot be negative");
        }

        if (errors.isEmpty()) {
            return ValidationResult.valid(record);
        } else {
            return ValidationResult.invalid(errors);
        }
    }

    /**
     * Checks if a record has all required fields for aggregation.
     * 
     * @param record the record to check
     * @return true if the record has required aggregation fields
     */
    public static boolean hasRequiredAggregationFields(WeatherRecord record) {
        return record != null 
                && record.getDate() != null
                && record.getLocationId() >= MIN_LOCATION_ID 
                && record.getLocationId() <= MAX_LOCATION_ID;
    }
}
