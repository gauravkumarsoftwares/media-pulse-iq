package com.java.security.paseto;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;

/**
 * Verifier for PASETO {@code v4.local} tokens — the internal, gateway-minted
 * token of the token-exchange design (Option C).
 *
 * <p>Decrypts + authenticates the token with the shared 32-byte symmetric key
 * ({@link PasetoV4Local}), then validates the registered claims
 * ({@code exp}/{@code nbf}/{@code iss}/{@code aud}) via {@link ClaimsValidator}.
 * Immutable and thread-safe.
 */
public final class PasetoV4LocalVerifier implements PasetoVerifier {

    private final byte[] key;
    private final String expectedIssuer;   // nullable -> not enforced
    private final String expectedAudience; // nullable -> not enforced
    private final Duration clockSkew;
    private final ObjectMapper mapper;

    public PasetoV4LocalVerifier(String keyMaterial,
                                 String expectedIssuer,
                                 String expectedAudience,
                                 Duration clockSkew) {
        this.key = SymmetricKeys.parse32(keyMaterial);
        this.expectedIssuer = ClaimsValidator.blankToNull(expectedIssuer);
        this.expectedAudience = ClaimsValidator.blankToNull(expectedAudience);
        this.clockSkew = clockSkew == null ? Duration.ofSeconds(60) : clockSkew;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public PasetoClaims verify(String token) {
        return verify(token, null);
    }

    public PasetoClaims verify(String token, byte[] implicitAssertion) {
        byte[] json = PasetoV4Local.decrypt(key, token, implicitAssertion);
        PasetoClaims claims = parseClaims(json);
        ClaimsValidator.validate(claims, expectedIssuer, expectedAudience, clockSkew);
        return claims;
    }

    private PasetoClaims parseClaims(byte[] json) {
        try {
            return mapper.readValue(json, PasetoClaims.class);
        } catch (Exception ex) {
            throw new PasetoException("Unable to parse token claims", ex);
        }
    }
}

