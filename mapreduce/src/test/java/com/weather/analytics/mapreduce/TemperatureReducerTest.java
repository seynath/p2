package com.weather.analytics.mapreduce;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TemperatureReducer logic.
 * Tests the core mean calculation logic without Hadoop context.
 */
class TemperatureReducerTest {

    @Test
    void testMeanSingleValue() {
        List<Double> values = Arrays.asList(26.0);
        
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        double mean = TemperatureReducer.calculateMean(sum, count);
        
        assertEquals(26.0, mean, 0.001);
    }

    @Test
    void testMeanMultipleValues() {
        List<Double> values = Arrays.asList(24.0, 26.0, 28.0);
        
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        double mean = TemperatureReducer.calculateMean(sum, count);
        
        assertEquals(26.0, mean, 0.001);
    }

    @Test
    void testMeanZeroCount() {
        double mean = TemperatureReducer.calculateMean(0.0, 0);
        
        assertEquals(0.0, mean, 0.001);
    }

    @Test
    void testMeanDecimalPrecision() {
        List<Double> values = Arrays.asList(25.5, 26.3, 27.1);
        
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        double mean = TemperatureReducer.calculateMean(sum, count);
        
        assertEquals(26.3, mean, 0.001);
    }

    @Test
    void testKeyParsing() {
        String key = "Colombo\t1\t2010";
        String[] keyParts = key.split("\t");
        
        assertEquals(3, keyParts.length);
        assertEquals("Colombo", keyParts[0]);
        assertEquals("1", keyParts[1]);
        assertEquals("2010", keyParts[2]);
    }

    @Test
    void testOutputFormat() {
        String district = "Colombo";
        int month = 1;
        int year = 2010;
        double meanTemperature = 26.35;
        int count = 31;
        
        String result = String.format("mean_temp=%.2f°C (records: %d)", meanTemperature, count);
        String outputKeyStr = String.format("%s\t%d\t%d", district, month, year);
        
        assertEquals("mean_temp=26.35°C (records: 31)", result);
        assertEquals("Colombo\t1\t2010", outputKeyStr);
    }

    @Test
    void testLargeMeanCalculation() {
        List<Double> values = Arrays.asList(
            25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0, 34.0,
            24.5, 25.5, 26.5, 27.5, 28.5, 29.5, 30.5, 31.5, 32.5, 33.5,
            24.0, 25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0, 33.0
        );
        
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        double mean = TemperatureReducer.calculateMean(sum, count);
        
        // Expected: (sum of all values) / 30
        double expectedSum = 25.0 + 26.0 + 27.0 + 28.0 + 29.0 + 30.0 + 31.0 + 32.0 + 33.0 + 34.0 +
                            24.5 + 25.5 + 26.5 + 27.5 + 28.5 + 29.5 + 30.5 + 31.5 + 32.5 + 33.5 +
                            24.0 + 25.0 + 26.0 + 27.0 + 28.0 + 29.0 + 30.0 + 31.0 + 32.0 + 33.0;
        double expectedMean = expectedSum / 30;
        
        assertEquals(expectedMean, mean, 0.001);
    }

    @Test
    void testNegativeTemperatures() {
        // While Sri Lanka doesn't have negative temps, test for robustness
        List<Double> values = Arrays.asList(-5.0, 0.0, 5.0);
        
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        double mean = TemperatureReducer.calculateMean(sum, count);
        
        assertEquals(0.0, mean, 0.001);
    }
}
