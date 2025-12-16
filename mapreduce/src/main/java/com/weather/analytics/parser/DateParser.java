package com.weather.analytics.parser;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Utility class for parsing dates in M/D/YYYY format from the weather dataset.
 * Handles both single and double digit month/day values.
 */
public class DateParser {
    
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("M/d/yyyy");

    /**
     * Parses a date string in M/D/YYYY format to LocalDate.
     * 
     * @param dateString the date string to parse (e.g., "1/1/2010" or "12/31/2024")
     * @return the parsed LocalDate
     * @throws DateTimeParseException if the date string is invalid
     * @throws IllegalArgumentException if the date string is null or empty
     */
    public static LocalDate parse(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            throw new IllegalArgumentException("Date string cannot be null or empty");
        }
        return LocalDate.parse(dateString.trim(), FORMATTER);
    }

    /**
     * Formats a LocalDate back to M/D/YYYY string format.
     * 
     * @param date the LocalDate to format
     * @return the formatted date string
     * @throws IllegalArgumentException if the date is null
     */
    public static String format(LocalDate date) {
        if (date == null) {
            throw new IllegalArgumentException("Date cannot be null");
        }
        return FORMATTER.format(date);
    }

    /**
     * Checks if a date string is valid in M/D/YYYY format.
     * 
     * @param dateString the date string to validate
     * @return true if valid, false otherwise
     */
    public static boolean isValid(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return false;
        }
        try {
            LocalDate.parse(dateString.trim(), FORMATTER);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
