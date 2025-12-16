package com.weather.analytics.mapreduce;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MaxPrecipitationReducer logic.
 * Tests the output formatting and ordinal conversion.
 */
class MaxPrecipitationReducerTest {

    private MaxPrecipitationReducer reducer;

    @BeforeEach
    void setUp() {
        reducer = new MaxPrecipitationReducer();
    }

    @Test
    void testOrdinalFirst() {
        assertEquals("1st", MaxPrecipitationReducer.getOrdinal(1));
    }

    @Test
    void testOrdinalSecond() {
        assertEquals("2nd", MaxPrecipitationReducer.getOrdinal(2));
    }

    @Test
    void testOrdinalThird() {
        assertEquals("3rd", MaxPrecipitationReducer.getOrdinal(3));
    }

    @Test
    void testOrdinalFourth() {
        assertEquals("4th", MaxPrecipitationReducer.getOrdinal(4));
    }

    @Test
    void testOrdinalEleventh() {
        // Special case: 11th, 12th, 13th use "th"
        assertEquals("11th", MaxPrecipitationReducer.getOrdinal(11));
    }

    @Test
    void testOrdinalTwelfth() {
        assertEquals("12th", MaxPrecipitationReducer.getOrdinal(12));
    }

    @Test
    void testOutputFormatJanuary() {
        String output = MaxPrecipitationReducer.formatOutput(1, 2015, 1234.56);
        assertEquals("1st month in 2015 had the highest total precipitation of 1234.56 hr", output);
    }

    @Test
    void testOutputFormatFebruary() {
        String output = MaxPrecipitationReducer.formatOutput(2, 2020, 500.00);
        assertEquals("2nd month in 2020 had the highest total precipitation of 500.00 hr", output);
    }

    @Test
    void testOutputFormatMarch() {
        String output = MaxPrecipitationReducer.formatOutput(3, 2018, 750.25);
        assertEquals("3rd month in 2018 had the highest total precipitation of 750.25 hr", output);
    }

    @Test
    void testOutputFormatApril() {
        String output = MaxPrecipitationReducer.formatOutput(4, 2012, 999.99);
        assertEquals("4th month in 2012 had the highest total precipitation of 999.99 hr", output);
    }

    @Test
    void testOutputFormatNovember() {
        String output = MaxPrecipitationReducer.formatOutput(11, 2023, 2000.00);
        assertEquals("11th month in 2023 had the highest total precipitation of 2000.00 hr", output);
    }

    @Test
    void testOutputFormatDecember() {
        String output = MaxPrecipitationReducer.formatOutput(12, 2010, 1500.50);
        assertEquals("12th month in 2010 had the highest total precipitation of 1500.50 hr", output);
    }

    @Test
    void testAllMonthOrdinals() {
        assertEquals("1st", MaxPrecipitationReducer.getOrdinal(1));
        assertEquals("2nd", MaxPrecipitationReducer.getOrdinal(2));
        assertEquals("3rd", MaxPrecipitationReducer.getOrdinal(3));
        assertEquals("4th", MaxPrecipitationReducer.getOrdinal(4));
        assertEquals("5th", MaxPrecipitationReducer.getOrdinal(5));
        assertEquals("6th", MaxPrecipitationReducer.getOrdinal(6));
        assertEquals("7th", MaxPrecipitationReducer.getOrdinal(7));
        assertEquals("8th", MaxPrecipitationReducer.getOrdinal(8));
        assertEquals("9th", MaxPrecipitationReducer.getOrdinal(9));
        assertEquals("10th", MaxPrecipitationReducer.getOrdinal(10));
        assertEquals("11th", MaxPrecipitationReducer.getOrdinal(11));
        assertEquals("12th", MaxPrecipitationReducer.getOrdinal(12));
    }

    @Test
    void testOutputFormatWithZeroPrecipitation() {
        String output = MaxPrecipitationReducer.formatOutput(6, 2019, 0.0);
        assertEquals("6th month in 2019 had the highest total precipitation of 0.00 hr", output);
    }

    @Test
    void testOutputFormatWithSmallPrecipitation() {
        String output = MaxPrecipitationReducer.formatOutput(7, 2021, 0.01);
        assertEquals("7th month in 2021 had the highest total precipitation of 0.01 hr", output);
    }

    @Test
    void testOutputFormatWithLargePrecipitation() {
        String output = MaxPrecipitationReducer.formatOutput(10, 2014, 99999.99);
        assertEquals("10th month in 2014 had the highest total precipitation of 99999.99 hr", output);
    }
}
