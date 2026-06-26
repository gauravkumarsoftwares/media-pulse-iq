package com.java.ingestion.common;

import org.springframework.http.HttpStatus;

/**
 * General-purpose runtime exception that carries an explicit HTTP status.
 * Thrown by service-layer components and mapped to {@link ApiErrorResponse}
 * by {@link GlobalExceptionHandler}.
 */
public class ApiException extends RuntimeException {

    private final HttpStatus status;

    public ApiException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}

