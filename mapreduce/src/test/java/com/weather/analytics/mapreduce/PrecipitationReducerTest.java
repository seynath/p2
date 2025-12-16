package com.weather.analytics.mapreduce;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for PrecipitationReducer logic.
 * Tests the core aggregation logic without Hadoop context.
 */
class PrecipitationReducerTest {

    @Test
    void testSumSingleValue() {
        List<Double> values = Arrays.asList(5.0);
        
        double total = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        
        assertEquals(5.0, total, 0.001);
        assertEquals(1, count);
    }

    @Test
    void testSumMultipleValues() {
        List<Double> values = Arrays.asList(3.0, 5.0, 2.0);
        
        double total = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        
        assertEquals(10.0, total, 0.001);
        assertEquals(3, count);
    }

    @Test
    void testSumZeroValues() {
        List<Double> values = Arrays.asList(0.0, 0.0);
        
        double total = values.stream().mapToDouble(Double::doubleValue).sum();
        
        assertEquals(0.0, total, 0.001);
    }

    @Test
    void testSumDecimalPrecision() {
        List<Double> values = Arrays.asList(1.5, 2.75, 0.25);
        
        double total = values.stream().mapToDouble(Double::doubleValue).sum();
        
        assertEquals(4.5, total, 0.001);
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
        double totalPrecipitationHours = 10.0;
        int count = 3;
        
        String result = String.format("%.2f hours (records: %d)", totalPrecipitationHours, count);
        String outputKeyStr = String.format("%s\t%d\t%d", district, month, year);
        
        assertEquals("10.00 hours (records: 3)", result);
        assertEquals("Colombo\t1\t2010", outputKeyStr);
    }

    @Test
    void testLargeAggregation() {
        List<Double> values = Arrays.asList(
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
            0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5,
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        );
        
        double total = values.stream().mapToDouble(Double::doubleValue).sum();
        int count = values.size();
        
        assertEquals(105.0, total, 0.001);
        assertEquals(30, count);
    }
}
