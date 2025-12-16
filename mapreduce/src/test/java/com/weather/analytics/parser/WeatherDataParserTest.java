package com.weather.analytics.parser;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.validation.ValidationResult;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WeatherDataParser.
 */
class WeatherDataParserTest {

    private static final String VALID_LINE = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,0,12.2,27.4,19,20.92,4.61,06:22,18:05";
    private static final String HEADER_LINE = "location_id,date,weather_code (wmo code),temperature_2m_max (°C),temperature_2m_min (°C),temperature_2m_mean (°C),apparent_temperature_max (°C),apparent_temperature_min (°C),apparent_temperature_mean (°C),daylight_duration (s),sunshine_duration (s),precipitation_sum (mm),rain_sum (mm),precipitation_hours (h),wind_speed_10m_max (km/h),wind_gusts_10m_max (km/h),wind_direction_10m_dominant (°),shortwave_radiation_sum (MJ/m²),et0_fao_evapotranspiration (mm),sunrise,sunset";

    @Test
    void parse_validLine_returnsWeatherRecord() {
        Optional<WeatherRecord> result = WeatherDataParser.parse(VALID_LINE);
        
        assertTrue(result.isPresent());
        WeatherRecord record = result.get();
        assertEquals(0, record.getLocationId());
        assertEquals(LocalDate.of(2010, 1, 1), record.getDate());
        assertEquals(1, record.getWeatherCode());
        assertEquals(30.1, record.getTemperature2mMax(), 0.01);
        assertEquals(22.6, record.getTemperature2mMin(), 0.01);
        assertEquals(26.0, record.getTemperature2mMean(), 0.01);
        assertEquals(0.0, record.getPrecipitationHours(), 0.01);
        assertEquals(20.92, record.getShortwaveRadiationSum(), 0.01);
        assertEquals(4.61, record.getEt0FaoEvapotranspiration(), 0.01);
    }

    @Test
    void parse_nullLine_returnsEmpty() {
        Optional<WeatherRecord> result = WeatherDataParser.parse(null);
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_emptyLine_returnsEmpty() {
        Optional<WeatherRecord> result = WeatherDataParser.parse("");
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_insufficientColumns_returnsEmpty() {
        Optional<WeatherRecord> result = WeatherDataParser.parse("0,1/1/2010,1");
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_invalidLocationId_returnsEmpty() {
        String invalidLine = "abc,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,0,12.2,27.4,19,20.92,4.61,06:22,18:05";
        Optional<WeatherRecord> result = WeatherDataParser.parse(invalidLine);
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_invalidDate_returnsEmpty() {
        String invalidLine = "0,invalid-date,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,0,12.2,27.4,19,20.92,4.61,06:22,18:05";
        Optional<WeatherRecord> result = WeatherDataParser.parse(invalidLine);
        assertTrue(result.isEmpty());
    }

    @Test
    void parse_withNullValues_handlesGracefully() {
        String lineWithNulls = "0,1/1/2010,1,,,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,0,12.2,27.4,19,20.92,4.61,06:22,18:05";
        Optional<WeatherRecord> result = WeatherDataParser.parse(lineWithNulls);
        
        assertTrue(result.isPresent());
        WeatherRecord record = result.get();
        assertNull(record.getTemperature2mMax());
        assertNull(record.getTemperature2mMin());
        assertEquals(26.0, record.getTemperature2mMean(), 0.01);
    }

    @Test
    void isHeader_headerLine_returnsTrue() {
        assertTrue(WeatherDataParser.isHeader(HEADER_LINE));
    }

    @Test
    void isHeader_dataLine_returnsFalse() {
        assertFalse(WeatherDataParser.isHeader(VALID_LINE));
    }

    @Test
    void isHeader_nullLine_returnsFalse() {
        assertFalse(WeatherDataParser.isHeader(null));
    }

    @Test
    void parseAndValidate_validLine_returnsValidResult() {
        ValidationResult<WeatherRecord> result = WeatherDataParser.parseAndValidate(VALID_LINE);
        
        assertTrue(result.isValid());
        assertTrue(result.getValue().isPresent());
    }

    @Test
    void parseAndValidate_invalidLine_returnsInvalidResult() {
        ValidationResult<WeatherRecord> result = WeatherDataParser.parseAndValidate("invalid");
        
        assertFalse(result.isValid());
        assertTrue(result.hasErrors());
    }
}
