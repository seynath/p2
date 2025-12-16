package com.weather.analytics.validation;

import com.weather.analytics.model.WeatherRecord;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WeatherDataValidator.
 */
class WeatherDataValidatorTest {

    @Test
    void validate_validRecord_returnsValid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .temperature2mMax(30.0)
                .temperature2mMin(22.0)
                .temperature2mMean(26.0)
                .precipitationHours(0.0)
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertTrue(result.isValid());
        assertFalse(result.hasErrors());
    }

    @Test
    void validate_nullDate_returnsInvalid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(null)
                .weatherCode(1)
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Date")));
    }

    @Test
    void validate_invalidLocationId_returnsInvalid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(99)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Location ID")));
    }

    @Test
    void validate_temperatureOutOfRange_returnsInvalid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .temperature2mMax(60.0) // Out of range
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Temperature")));
    }

    @Test
    void validate_maxLessThanMin_returnsInvalid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .temperature2mMax(20.0)
                .temperature2mMin(25.0) // Min > Max
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Max temperature")));
    }

    @Test
    void validate_negativePrecipitation_returnsInvalid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .precipitationHours(-5.0)
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("Precipitation")));
    }

    @Test
    void validate_negativeRadiation_returnsInvalid() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .shortwaveRadiationSum(-10.0)
                .build();

        ValidationResult<WeatherRecord> result = WeatherDataValidator.validate(record);
        
        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream().anyMatch(e -> e.contains("radiation")));
    }

    @Test
    void hasRequiredAggregationFields_validRecord_returnsTrue() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(LocalDate.of(2010, 1, 1))
                .weatherCode(1)
                .build();

        assertTrue(WeatherDataValidator.hasRequiredAggregationFields(record));
    }

    @Test
    void hasRequiredAggregationFields_nullDate_returnsFalse() {
        WeatherRecord record = WeatherRecord.builder()
                .locationId(0)
                .date(null)
                .weatherCode(1)
                .build();

        assertFalse(WeatherDataValidator.hasRequiredAggregationFields(record));
    }

    @Test
    void hasRequiredAggregationFields_nullRecord_returnsFalse() {
        assertFalse(WeatherDataValidator.hasRequiredAggregationFields(null));
    }
}
