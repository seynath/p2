package com.weather.analytics.parser;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DateParser.
 */
class DateParserTest {

    @Test
    void parse_validSingleDigitMonthDay_returnsCorrectDate() {
        LocalDate result = DateParser.parse("1/1/2010");
        assertEquals(LocalDate.of(2010, 1, 1), result);
    }

    @Test
    void parse_validDoubleDigitMonthDay_returnsCorrectDate() {
        LocalDate result = DateParser.parse("12/31/2024");
        assertEquals(LocalDate.of(2024, 12, 31), result);
    }

    @Test
    void parse_mixedDigits_returnsCorrectDate() {
        LocalDate result = DateParser.parse("1/15/2020");
        assertEquals(LocalDate.of(2020, 1, 15), result);
    }

    @Test
    void parse_withWhitespace_trimsAndParses() {
        LocalDate result = DateParser.parse("  1/1/2010  ");
        assertEquals(LocalDate.of(2010, 1, 1), result);
    }

    @Test
    void parse_nullInput_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> DateParser.parse(null));
    }

    @Test
    void parse_emptyInput_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> DateParser.parse(""));
    }

    @Test
    void parse_invalidFormat_throwsException() {
        assertThrows(DateTimeParseException.class, () -> DateParser.parse("2010-01-01"));
    }

    @Test
    void format_validDate_returnsCorrectString() {
        LocalDate date = LocalDate.of(2010, 1, 1);
        String result = DateParser.format(date);
        assertEquals("1/1/2010", result);
    }

    @Test
    void format_doubleDigitMonthDay_returnsCorrectString() {
        LocalDate date = LocalDate.of(2024, 12, 31);
        String result = DateParser.format(date);
        assertEquals("12/31/2024", result);
    }

    @Test
    void format_nullDate_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> DateParser.format(null));
    }

    @Test
    void isValid_validDate_returnsTrue() {
        assertTrue(DateParser.isValid("1/1/2010"));
        assertTrue(DateParser.isValid("12/31/2024"));
    }

    @Test
    void isValid_invalidDate_returnsFalse() {
        assertFalse(DateParser.isValid(null));
        assertFalse(DateParser.isValid(""));
        assertFalse(DateParser.isValid("2010-01-01"));
        assertFalse(DateParser.isValid("invalid"));
    }
}
