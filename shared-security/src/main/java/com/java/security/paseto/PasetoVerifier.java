package com.java.security.paseto;

/**
 * Common contract for in-service PASETO verification, so the authentication
 * filter is agnostic to the token flavour ({@code v4.public} for direct
 * edge-token forwarding, or {@code v4.local} for the internal token-exchange
 * design — Option C).
 */
public interface PasetoVerifier {

    /**
     * Verify a token and return its validated claims.
     *
     * @throws PasetoException if the token is malformed, fails its
     *                         signature/authentication check, or fails
     *                         temporal/issuer/audience validation
     */
    PasetoClaims verify(String token);
}

