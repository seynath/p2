package com.weather.analytics.parser;

import com.weather.analytics.model.LocationRecord;
import com.weather.analytics.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LocationDataParser.
 */
class LocationDataParserTest {

    private static final String VALID_LINE = "0,6.924429,79.90725,4,19800,Asia/Colombo,530,Colombo";
    private static final String HEADER_LINE = "location_id,latitude,longitude,elevation,utc_offset_seconds,timezone,timezone_abbreviation,city_name";

    @Test
    void parse_validLine_returnsLocationRecord() {
        Optional<LocationRecord> result = LocationDataParser.parse(VALID_LINE);
        
        assertTrue(result.isPresent());
        LocationRecord record = result.get();
        assertEquals(0, record.getLocationId());
        assertEquals(6.924429, record.getLatitude(), 0.0001);
        assertEquals(79.90725, record.getLongitude(), 0.0001);
        assertEquals(4, record.getElevation());
        assertEquals(19800, record.getUtcOffsetSeconds());
        assertEquals("Asia/Colombo", record.getTimezone());
        assertEquals("530", record.getTimezoneAbbreviation());
        assertEquals("Colombo", record.getCityName());
    }

    @Test
    void parse_allDistricts_parsesCorrectly() {
        String[] districts = {
            "0,6.924429,79.90725,4,19800,Asia/Colombo,530,Colombo",
            "1,7.0650263,79.96622,19,19800,Asia/Colombo,530,Gampaha",
            "5,6.9947276,80.73418,1865,19800,Asia/Colombo,530,Nuwara Eliya",
            "18,8.04921,79.84615,4,19800,Asia/Colombo,530,Puttalam"
        };
        
        for (String line : districts) {
            Optional<LocationRecord> result = LocationDataParser.parse(line);
            assertTrue(result.isPresent(), "Failed to parse: " + line);
        }
    }

    @Test
    void parse_nullLine_returnsEmpty() {
        Optional<LocationRecord> result = LocationDataParser.parse(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_emptyLine_returnsEmpty() {
        Optional<LocationRecord> result = LocationDataParser.parse("");
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_insufficientColumns_returnsEmpty() {
        Optional<LocationRecord> result = LocationDataParser.parse("0,6.924429,79.90725");
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_invalidLocationId_returnsEmpty() {
        String invalidLine = "abc,6.924429,79.90725,4,19800,Asia/Colombo,530,Colombo";
        Optional<LocationRecord> result = LocationDataParser.parse(invalidLine);
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_invalidLatitude_returnsEmpty() {
        String invalidLine = "0,invalid,79.90725,4,19800,Asia/Colombo,530,Colombo";
        Optional<LocationRecord> result = LocationDataParser.parse(invalidLine);
        assertTrue(result.isEmpty());
    }

    @Test
    void isHeader_headerLine_returnsTrue() {
        assertTrue(LocationDataParser.isHeader(HEADER_LINE));
    }

    @Test
    void isHeader_dataLine_returnsFalse() {
        assertFalse(LocationDataParser.isHeader(VALID_LINE));
    }

    @Test
    void isHeader_nullLine_returnsFalse() {
        assertFalse(LocationDataParser.isHeader(null));
    }

    @Test
    void parseAndValidate_validLine_returnsValidResult() {
        ValidationResult<LocationRecord> result = LocationDataParser.parseAndValidate(VALID_LINE);
        
        assertTrue(result.isValid());
        assertTrue(result.getValue().isPresent());
    }

    @Test
    void parseAndValidate_invalidLine_returnsInvalidResult() {
        ValidationResult<LocationRecord> result = LocationDataParser.parseAndValidate("invalid");
        
        assertFalse(result.isValid());
        assertTrue(result.hasErrors());
    }
}
