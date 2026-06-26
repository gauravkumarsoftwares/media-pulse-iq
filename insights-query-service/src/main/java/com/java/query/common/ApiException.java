package com.java.query.common;

import org.springframework.http.HttpStatus;

/**
 * General-purpose runtime exception carrying an explicit HTTP status.
 * Thrown by service components; caught and formatted by {@link GlobalExceptionHandler}.
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

