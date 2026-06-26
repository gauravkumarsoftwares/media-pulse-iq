package com.java.ingestion.common;

import java.time.Instant;
import java.util.List;

/**
 * Standardized error envelope returned by {@link GlobalExceptionHandler} for
 * all 4xx / 5xx responses. Provides a consistent shape that clients and
 * monitoring tools can parse reliably.
 *
 * @param timestamp    ISO-8601 instant at which the error occurred
 * @param status       HTTP status code
 * @param error        HTTP status reason phrase
 * @param message      Human-readable description of the failure
 * @param path         Request URI that triggered the error
 * @param fieldErrors  Per-field validation failures (non-empty only for 422)
 */
public record ApiErrorResponse(
        String timestamp,
        int status,
        String error,
        String message,
        String path,
        List<FieldError> fieldErrors) {

    /** Per-field constraint violation detail. */
    public record FieldError(String field, String message) {}

    /** Convenience factory — no field errors. */
    public static ApiErrorResponse of(int status, String error, String message, String path) {
        return new ApiErrorResponse(Instant.now().toString(), status, error, message, path, List.of());
    }

    /** Convenience factory — with field errors (422 Unprocessable Entity). */
    public static ApiErrorResponse ofValidation(String path, List<FieldError> fieldErrors) {
        return new ApiErrorResponse(
                Instant.now().toString(), 422, "Unprocessable Entity",
                "Request validation failed", path, fieldErrors);
    }
}

