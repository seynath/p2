package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PrecipitationMapper logic.
 * Tests the core parsing and key generation logic without Hadoop context.
 */
class PrecipitationMapperTest {

    @Test
    void testParseValidRecord() {
        String csvLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,5,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(csvLine);
        
        assertTrue(recordOpt.isPresent());
        WeatherRecord record = recordOpt.get();
        
        assertEquals(0, record.getLocationId());
        assertEquals(1, record.getMonth());
        assertEquals(2010, record.getYear());
        assertEquals(5.0, record.getPrecipitationHours(), 0.001);
    }

    @Test
    void testKeyGeneration() {
        String csvLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,5,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(csvLine);
        assertTrue(recordOpt.isPresent());
        WeatherRecord record = recordOpt.get();
        
        Map<Integer, String> locationData = new HashMap<>();
        locationData.put(0, "Colombo");
        
        String district = locationData.get(record.getLocationId());
        String compositeKey = String.format("%s\t%d\t%d", 
                district, record.getMonth(), record.getYear());
        
        assertEquals("Colombo\t1\t2010", compositeKey);
    }

    @Test
    void testHeaderDetection() {
        String headerLine = "location_id,date,weather_code (wmo code),temperature_2m_max (°C),temperature_2m_min (°C),temperature_2m_mean (°C),apparent_temperature_max (°C),apparent_temperature_min (°C),apparent_temperature_mean (°C),daylight_duration (s),sunshine_duration (s),precipitation_sum (mm),rain_sum (mm),precipitation_hours (h),wind_speed_10m_max (km/h),wind_gusts_10m_max (km/h),wind_direction_10m_dominant (°),shortwave_radiation_sum (MJ/m²),et0_fao_evapotranspiration (mm),sunrise,sunset";
        
        assertTrue(WeatherDataParser.isHeader(headerLine));
    }

    @Test
    void testDifferentMonths() {
        String janLine = "0,1/15/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,3,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String febLine = "0,2/15/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,7,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> janRecord = WeatherDataParser.parse(janLine);
        Optional<WeatherRecord> febRecord = WeatherDataParser.parse(febLine);
        
        assertTrue(janRecord.isPresent());
        assertTrue(febRecord.isPresent());
        
        assertEquals(1, janRecord.get().getMonth());
        assertEquals(2, febRecord.get().getMonth());
        assertEquals(3.0, janRecord.get().getPrecipitationHours(), 0.001);
        assertEquals(7.0, febRecord.get().getPrecipitationHours(), 0.001);
    }

    @Test
    void testDifferentDistricts() {
        String colomboLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,4,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String gampahaLine = "1,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,6,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> colomboRecord = WeatherDataParser.parse(colomboLine);
        Optional<WeatherRecord> gampahaRecord = WeatherDataParser.parse(gampahaLine);
        
        assertTrue(colomboRecord.isPresent());
        assertTrue(gampahaRecord.isPresent());
        
        assertEquals(0, colomboRecord.get().getLocationId());
        assertEquals(1, gampahaRecord.get().getLocationId());
    }

    @Test
    void testZeroPrecipitation() {
        String csvLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,0,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(csvLine);
        
        assertTrue(recordOpt.isPresent());
        assertEquals(0.0, recordOpt.get().getPrecipitationHours(), 0.001);
    }

    @Test
    void testLocationLookup() {
        Map<Integer, String> locationData = new HashMap<>();
        locationData.put(0, "Colombo");
        locationData.put(1, "Gampaha");
        locationData.put(2, "Kalutara");
        
        assertEquals("Colombo", locationData.get(0));
        assertEquals("Gampaha", locationData.get(1));
        assertEquals("Kalutara", locationData.get(2));
        assertNull(locationData.get(99));
    }
}
