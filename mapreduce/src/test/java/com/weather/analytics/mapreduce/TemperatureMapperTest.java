package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TemperatureMapper logic.
 * Tests the core parsing and key generation logic without Hadoop context.
 */
class TemperatureMapperTest {

    @Test
    void testParseValidRecordWithTemperature() {
        String csvLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,5,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(csvLine);
        
        assertTrue(recordOpt.isPresent());
        WeatherRecord record = recordOpt.get();
        
        assertEquals(0, record.getLocationId());
        assertEquals(1, record.getMonth());
        assertEquals(2010, record.getYear());
        assertEquals(26.0, record.getTemperature2mMean(), 0.001);
    }

    @Test
    void testKeyGenerationForTemperature() {
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
    void testDifferentTemperatures() {
        String coldLine = "0,1/15/2010,1,25.0,18.0,21.5,28.0,20.0,24.0,42220.2,38905.73,0.0,0.0,3,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String hotLine = "0,2/15/2010,1,35.0,28.0,31.5,38.0,30.0,34.0,42220.2,38905.73,0.0,0.0,7,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> coldRecord = WeatherDataParser.parse(coldLine);
        Optional<WeatherRecord> hotRecord = WeatherDataParser.parse(hotLine);
        
        assertTrue(coldRecord.isPresent());
        assertTrue(hotRecord.isPresent());
        
        assertEquals(21.5, coldRecord.get().getTemperature2mMean(), 0.001);
        assertEquals(31.5, hotRecord.get().getTemperature2mMean(), 0.001);
    }

    @Test
    void testDifferentDistrictsForTemperature() {
        String colomboLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,4,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String gampahaLine = "1,1/1/2010,1,29.5,21.8,25.5,33.5,24.0,28.0,42220.2,38905.73,0.0,0.0,6,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> colomboRecord = WeatherDataParser.parse(colomboLine);
        Optional<WeatherRecord> gampahaRecord = WeatherDataParser.parse(gampahaLine);
        
        assertTrue(colomboRecord.isPresent());
        assertTrue(gampahaRecord.isPresent());
        
        assertEquals(0, colomboRecord.get().getLocationId());
        assertEquals(1, gampahaRecord.get().getLocationId());
        assertEquals(26.0, colomboRecord.get().getTemperature2mMean(), 0.001);
        assertEquals(25.5, gampahaRecord.get().getTemperature2mMean(), 0.001);
    }

    @Test
    void testLocationLookupForTemperature() {
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
