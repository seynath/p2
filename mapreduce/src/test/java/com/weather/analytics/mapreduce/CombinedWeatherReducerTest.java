package com.weather.analytics.mapreduce;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for CombinedWeatherReducer logic.
 * Tests the combined aggregation and output formatting logic.
 */
class CombinedWeatherReducerTest {

    @Test
    void testMonthOrdinal1st() {
        assertEquals("1st", CombinedWeatherReducer.getMonthOrdinal(1));
    }

    @Test
    void testMonthOrdinal2nd() {
        assertEquals("2nd", CombinedWeatherReducer.getMonthOrdinal(2));
    }

    @Test
    void testMonthOrdinal3rd() {
        assertEquals("3rd", CombinedWeatherReducer.getMonthOrdinal(3));
    }

    @Test
    void testMonthOrdinal4thTo10th() {
        assertEquals("4th", CombinedWeatherReducer.getMonthOrdinal(4));
        assertEquals("5th", CombinedWeatherReducer.getMonthOrdinal(5));
        assertEquals("6th", CombinedWeatherReducer.getMonthOrdinal(6));
        assertEquals("7th", CombinedWeatherReducer.getMonthOrdinal(7));
        assertEquals("8th", CombinedWeatherReducer.getMonthOrdinal(8));
        assertEquals("9th", CombinedWeatherReducer.getMonthOrdinal(9));
        assertEquals("10th", CombinedWeatherReducer.getMonthOrdinal(10));
    }

    @Test
    void testMonthOrdinal11thAnd12th() {
        assertEquals("11th", CombinedWeatherReducer.getMonthOrdinal(11));
        assertEquals("12th", CombinedWeatherReducer.getMonthOrdinal(12));
    }

    @Test
    void testCalculateMean() {
        assertEquals(26.0, CombinedWeatherReducer.calculateMean(78.0, 3), 0.001);
        assertEquals(0.0, CombinedWeatherReducer.calculateMean(0.0, 0), 0.001);
        assertEquals(25.5, CombinedWeatherReducer.calculateMean(51.0, 2), 0.001);
    }

    @Test
    void testOutputFormat() {
        String district = "Colombo";
        double totalPrecipitation = 45.50;
        double meanTemperature = 26.35;
        String monthOrdinal = CombinedWeatherReducer.getMonthOrdinal(1);
        int year = 2010;
        
        String result = String.format("%s had a total precipitation of %.2f hours with a mean temperature of %.2f°C for %s month in %d",
                district, totalPrecipitation, meanTemperature, monthOrdinal, year);
        
        assertEquals("Colombo had a total precipitation of 45.50 hours with a mean temperature of 26.35°C for 1st month in 2010", result);
    }

    @Test
    void testParsePrecipitationValue() {
        String valueStr = "P:5.5";
        assertTrue(valueStr.startsWith(CombinedWeatherMapper.PRECIPITATION_PREFIX));
        
        String precipStr = valueStr.substring(CombinedWeatherMapper.PRECIPITATION_PREFIX.length());
        double precipitation = Double.parseDouble(precipStr);
        
        assertEquals(5.5, precipitation, 0.001);
    }

    @Test
    void testParseTemperatureValue() {
        String valueStr = "T:26.5";
        assertTrue(valueStr.startsWith(CombinedWeatherMapper.TEMPERATURE_PREFIX));
        
        String tempStr = valueStr.substring(CombinedWeatherMapper.TEMPERATURE_PREFIX.length());
        double temperature = Double.parseDouble(tempStr);
        
        assertEquals(26.5, temperature, 0.001);
    }

    @Test
    void testKeyParsing() {
        String key = "Colombo\t1\t2010";
        String[] keyParts = key.split("\t");
        
        assertEquals(3, keyParts.length);
        assertEquals("Colombo", keyParts[0]);
        assertEquals(1, Integer.parseInt(keyParts[1]));
        assertEquals(2010, Integer.parseInt(keyParts[2]));
    }
}
