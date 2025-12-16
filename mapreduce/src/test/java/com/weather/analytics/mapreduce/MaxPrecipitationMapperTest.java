package com.weather.analytics.mapreduce;

import com.weather.analytics.model.WeatherRecord;
import com.weather.analytics.parser.WeatherDataParser;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MaxPrecipitationMapper logic.
 * Tests the core parsing and key generation logic without Hadoop context.
 */
class MaxPrecipitationMapperTest {

    @Test
    void testKeyGenerationMonthYear() {
        String csvLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,5,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(csvLine);
        assertTrue(recordOpt.isPresent());
        WeatherRecord record = recordOpt.get();
        
        // Key should be month\tyear (no district)
        String compositeKey = String.format("%d\t%d", record.getMonth(), record.getYear());
        
        assertEquals("1\t2010", compositeKey);
    }

    @Test
    void testDifferentMonthsGenerateDifferentKeys() {
        String janLine = "0,1/15/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,3,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String febLine = "0,2/15/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,7,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> janRecord = WeatherDataParser.parse(janLine);
        Optional<WeatherRecord> febRecord = WeatherDataParser.parse(febLine);
        
        assertTrue(janRecord.isPresent());
        assertTrue(febRecord.isPresent());
        
        String janKey = String.format("%d\t%d", janRecord.get().getMonth(), janRecord.get().getYear());
        String febKey = String.format("%d\t%d", febRecord.get().getMonth(), febRecord.get().getYear());
        
        assertEquals("1\t2010", janKey);
        assertEquals("2\t2010", febKey);
        assertNotEquals(janKey, febKey);
    }

    @Test
    void testDifferentYearsGenerateDifferentKeys() {
        String year2010Line = "0,5/15/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,3,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String year2015Line = "0,5/15/2015,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,7,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> record2010 = WeatherDataParser.parse(year2010Line);
        Optional<WeatherRecord> record2015 = WeatherDataParser.parse(year2015Line);
        
        assertTrue(record2010.isPresent());
        assertTrue(record2015.isPresent());
        
        String key2010 = String.format("%d\t%d", record2010.get().getMonth(), record2010.get().getYear());
        String key2015 = String.format("%d\t%d", record2015.get().getMonth(), record2015.get().getYear());
        
        assertEquals("5\t2010", key2010);
        assertEquals("5\t2015", key2015);
        assertNotEquals(key2010, key2015);
    }

    @Test
    void testSameMonthYearDifferentDistrictsGenerateSameKey() {
        // Different districts (location_id 0 and 1) but same month/year should have same key
        String colomboLine = "0,3/10/2012,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,4,12.2,27.4,19,20.92,4.61,06:22,18:05";
        String gampahaLine = "1,3/20/2012,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,6,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> colomboRecord = WeatherDataParser.parse(colomboLine);
        Optional<WeatherRecord> gampahaRecord = WeatherDataParser.parse(gampahaLine);
        
        assertTrue(colomboRecord.isPresent());
        assertTrue(gampahaRecord.isPresent());
        
        // Different location IDs
        assertNotEquals(colomboRecord.get().getLocationId(), gampahaRecord.get().getLocationId());
        
        // But same key (month-year)
        String colomboKey = String.format("%d\t%d", colomboRecord.get().getMonth(), colomboRecord.get().getYear());
        String gampahaKey = String.format("%d\t%d", gampahaRecord.get().getMonth(), gampahaRecord.get().getYear());
        
        assertEquals(colomboKey, gampahaKey);
        assertEquals("3\t2012", colomboKey);
    }

    @Test
    void testPrecipitationValueExtraction() {
        String csvLine = "0,1/1/2010,1,30.1,22.6,26.0,34.5,25.0,29.0,42220.2,38905.73,0.0,0.0,8.5,12.2,27.4,19,20.92,4.61,06:22,18:05";
        
        Optional<WeatherRecord> recordOpt = WeatherDataParser.parse(csvLine);
        assertTrue(recordOpt.isPresent());
        
        assertEquals(8.5, recordOpt.get().getPrecipitationHours(), 0.001);
    }

    @Test
    void testHeaderDetection() {
        String headerLine = "location_id,date,weather_code (wmo code),temperature_2m_max (°C),temperature_2m_min (°C),temperature_2m_mean (°C),apparent_temperature_max (°C),apparent_temperature_min (°C),apparent_temperature_mean (°C),daylight_duration (s),sunshine_duration (s),precipitation_sum (mm),rain_sum (mm),precipitation_hours (h),wind_speed_10m_max (km/h),wind_gusts_10m_max (km/h),wind_direction_10m_dominant (°),shortwave_radiation_sum (MJ/m²),et0_fao_evapotranspiration (mm),sunrise,sunset";
        
        assertTrue(WeatherDataParser.isHeader(headerLine));
    }
}
