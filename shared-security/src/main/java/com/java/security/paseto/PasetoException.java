package com.java.security.paseto;

/**
 * Raised when a PASETO token fails verification (bad format, invalid signature,
 * expired, wrong issuer/audience, etc.). Never carries the raw token to avoid
 * leaking credentials into logs.
 */
public class PasetoException extends RuntimeException {

    public PasetoException(String message) {
        super(message);
    }

    public PasetoException(String message, Throwable cause) {
        super(message, cause);
    }
}
