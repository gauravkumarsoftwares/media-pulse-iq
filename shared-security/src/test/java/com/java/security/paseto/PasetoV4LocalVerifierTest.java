package com.java.security.paseto;

import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip tests for the internal {@code v4.local} token-exchange path:
 * mint with {@link PasetoV4LocalIssuer} / {@link PasetoV4Local}, verify with
 * {@link PasetoV4LocalVerifier}, and assert tamper/expiry/wrong-key rejection.
 */
class PasetoV4LocalVerifierTest {

    private static String newKeyBase64() {
        byte[] k = new byte[32];
        new SecureRandom().nextBytes(k);
        return Base64.getEncoder().encodeToString(k);
    }

    @Test
    void verifiesIssuedToken() {
        String key = newKeyBase64();
        PasetoV4LocalIssuer issuer = new PasetoV4LocalIssuer(key, "edge", "media-pulse-iq");
        String token = issuer.issue("walmart_us", List.of("read:ads"),
                List.of("cmp_1"), Duration.ofSeconds(60));

        PasetoV4LocalVerifier verifier =
                new PasetoV4LocalVerifier(key, "edge", "media-pulse-iq", Duration.ofSeconds(0));
        PasetoClaims claims = verifier.verify(token);

        assertEquals("walmart_us", claims.tenantId());
        assertTrue(claims.hasScope("read:ads"));
        assertTrue(claims.canAccessCampaign("cmp_1"));
        assertFalse(claims.canAccessCampaign("cmp_other"));
    }

    @Test
    void roundTripsRawPayload() {
        byte[] key = SymmetricKeys.parse32(newKeyBase64());
        String json = "{\"tenant_id\":\"t\",\"exp\":\"" + java.time.Instant.now().plusSeconds(60) + "\"}";
        String token = PasetoV4Local.encrypt(key, json.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        byte[] back = PasetoV4Local.decrypt(key, token);
        assertEquals(json, new String(back, java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void rejectsTamperedToken() {
        String key = newKeyBase64();
        String token = new PasetoV4LocalIssuer(key, null, null)
                .issue("t", List.of(), List.of(), Duration.ofSeconds(60));

        String[] parts = token.split("\\.");
        parts[2] = parts[2].substring(0, parts[2].length() - 2)
                + (parts[2].endsWith("A") ? "B" : "A");
        String tampered = String.join(".", parts);

        PasetoV4LocalVerifier verifier =
                new PasetoV4LocalVerifier(key, null, null, Duration.ofSeconds(0));
        assertThrows(PasetoException.class, () -> verifier.verify(tampered));
    }

    @Test
    void rejectsExpiredToken() {
        String key = newKeyBase64();
        String token = new PasetoV4LocalIssuer(key, null, null)
                .issue("t", List.of(), List.of(), Duration.ofSeconds(-3600));

        PasetoV4LocalVerifier verifier =
                new PasetoV4LocalVerifier(key, null, null, Duration.ofSeconds(0));
        assertThrows(PasetoException.class, () -> verifier.verify(token));
    }

    @Test
    void rejectsWrongKey() {
        String token = new PasetoV4LocalIssuer(newKeyBase64(), null, null)
                .issue("t", List.of(), List.of(), Duration.ofSeconds(60));

        PasetoV4LocalVerifier verifier =
                new PasetoV4LocalVerifier(newKeyBase64(), null, null, Duration.ofSeconds(0));
        assertThrows(PasetoException.class, () -> verifier.verify(token));
    }

    @Test
    void rejectsWrongIssuer() {
        String key = newKeyBase64();
        String token = new PasetoV4LocalIssuer(key, "evil", null)
                .issue("t", List.of(), List.of(), Duration.ofSeconds(60));

        PasetoV4LocalVerifier verifier =
                new PasetoV4LocalVerifier(key, "edge", null, Duration.ofSeconds(0));
        assertThrows(PasetoException.class, () -> verifier.verify(token));
    }
}

