package com.weather.analytics.validation;

import com.weather.analytics.model.LocationRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LocationDataValidator.
 */
class LocationDataValidatorTest {

    @Test
    void validate_validRecord_returnsValid() {
        LocationRecord record = new LocationRecord(
                0, 6.924429, 79.90725, 4, 19800, "Asia/Colombo", "530", "Colombo"
        );

        ValidationResult<LocationRecord> result = LocationDataValidator.validate(record);
        
        assertTrue(result.isValid());
        assertFalse(result.hasErrors());
    }

    @Test
    void validate_invalidLocationId_returnsInvalid() {
        LocationRecord record = new LocationRecord(
                99, 6.924429, 79.90725, 4, 19800, "Asia/Colombo", "530", "Colombo"
        );

        ValidationResult<LocationRecord> result = LocationDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Location ID")));
    }

    @Test
    void validate_latitudeOutOfBounds_returnsInvalid() {
        LocationRecord record = new LocationRecord(
                0, 1.0, 79.90725, 4, 19800, "Asia/Colombo", "530", "Colombo"
        );

        ValidationResult<LocationRecord> result = LocationDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Latitude")));
    }

    @Test
    void validate_longitudeOutOfBounds_returnsInvalid() {
        LocationRecord record = new LocationRecord(
                0, 6.924429, 90.0, 4, 19800, "Asia/Colombo", "530", "Colombo"
        );

        ValidationResult<LocationRecord> result = LocationDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Longitude")));
    }

    @Test
    void validate_emptyCityName_returnsInvalid() {
        LocationRecord record = new LocationRecord(
                0, 6.924429, 79.90725, 4, 19800, "Asia/Colombo", "530", ""
        );

        ValidationResult<LocationRecord> result = LocationDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("City name")));
    }

    @Test
    void validate_emptyTimezone_returnsInvalid() {
        LocationRecord record = new LocationRecord(
                0, 6.924429, 79.90725, 4, 19800, "", "530", "Colombo"
        );

        ValidationResult<LocationRecord> result = LocationDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Timezone")));
    }

    @Test
    void isValidLocationId_validIds_returnsTrue() {
        assertTrue(LocationDataValidator.isValidLocationId(0));
        assertTrue(LocationDataValidator.isValidLocationId(9));
        assertTrue(LocationDataValidator.isValidLocationId(18));
    }

    @Test
    void isValidLocationId_invalidIds_returnsFalse() {
        assertFalse(LocationDataValidator.isValidLocationId(-1));
        assertFalse(LocationDataValidator.isValidLocationId(19));
        assertFalse(LocationDataValidator.isValidLocationId(100));
    }
}
