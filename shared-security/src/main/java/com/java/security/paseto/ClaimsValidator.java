package com.java.security.paseto;

import java.time.Duration;
import java.time.Instant;

/**
 * Shared registered-claim validation ({@code exp} / {@code nbf} / {@code iss} /
 * {@code aud}) reused by every PASETO verifier so the rules cannot drift between
 * the {@code v4.public} and {@code v4.local} paths.
 */
final class ClaimsValidator {

    private ClaimsValidator() {
    }

    static void validate(PasetoClaims claims,
                         String expectedIssuer,
                         String expectedAudience,
                         Duration clockSkew) {
        Duration skew = clockSkew == null ? Duration.ofSeconds(60) : clockSkew;
        Instant now = Instant.now();

        Instant exp = claims.expirationInstant();
        if (exp != null && now.minus(skew).isAfter(exp)) {
            throw new PasetoException("Token expired");
        }
        Instant nbf = claims.notBeforeInstant();
        if (nbf != null && now.plus(skew).isBefore(nbf)) {
            throw new PasetoException("Token not yet valid");
        }
        if (expectedIssuer != null && !expectedIssuer.equals(claims.issuer())) {
            throw new PasetoException("Unexpected token issuer");
        }
        if (expectedAudience != null && !expectedAudience.equals(claims.audience())) {
            throw new PasetoException("Unexpected token audience");
        }
    }

    static String blankToNull(String value) {
        return (value == null || value.isBlank()) ? null : value;
    }
}

