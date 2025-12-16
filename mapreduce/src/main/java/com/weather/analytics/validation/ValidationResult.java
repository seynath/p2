package com.weather.analytics.validation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Represents the result of a validation operation.
 * Contains either a valid value or a list of validation errors.
 * 
 * @param <T> the type of the validated value
 */
public class ValidationResult<T> {
    private final T value;
    private final List<String> errors;
    private final boolean valid;

    private ValidationResult(T value, List<String> errors, boolean valid) {
        this.value = value;
        this.errors = errors != null ? new ArrayList<>(errors) : new ArrayList<>();
        this.valid = valid;
    }

    /**
     * Creates a valid result with the given value.
     */
    public static <T> ValidationResult<T> valid(T value) {
        return new ValidationResult<>(value, Collections.emptyList(), true);
    }

    /**
     * Creates an invalid result with a single error message.
     */
    public static <T> ValidationResult<T> invalid(String error) {
        return new ValidationResult<>(null, List.of(error), false);
    }

    /**
     * Creates an invalid result with multiple error messages.
     */
    public static <T> ValidationResult<T> invalid(List<String> errors) {
        return new ValidationResult<>(null, errors, false);
    }

    /**
     * Creates a result with a value but also warnings/errors.
     */
    public static <T> ValidationResult<T> withWarnings(T value, List<String> warnings) {
        return new ValidationResult<>(value, warnings, true);
    }

    public boolean isValid() {
        return valid;
    }

    public Optional<T> getValue() {
        return Optional.ofNullable(value);
    }

    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    public boolean hasErrors() {
        return !errors.isEmpty();
    }

    @Override
    public String toString() {
        if (valid) {
            return "ValidationResult{valid=true, value=" + value + "}";
        } else {
            return "ValidationResult{valid=false, errors=" + errors + "}";
        }
    }
}
