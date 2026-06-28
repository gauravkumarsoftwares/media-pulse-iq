package com.java.security.paseto;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip tests: mint a real v4.public token with a freshly generated
 * Ed25519 key and assert the verifier accepts valid tokens and rejects
 * tampered / expired / wrong-key ones.
 */
class PasetoV4PublicVerifierTest {

    private static final String HEADER = "v4.public.";
    private static final Base64.Encoder URL = Base64.getUrlEncoder().withoutPadding();

    @Test
    void verifiesValidToken() throws Exception {
        KeyPair kp = generateEd25519();
        String json = """
                {"tenant_id":"walmart_us","scopes":["read:ads"],
                 "allowed_campaigns":["cmp_1"],"iss":"auth","aud":"event-analysis",
                 "exp":"%s"}""".formatted(Instant.now().plusSeconds(300));

        String token = mint(json, kp);
        PasetoV4PublicVerifier verifier = verifier(kp, "auth", "event-analysis");

        PasetoClaims claims = verifier.verify(token);
        assertEquals("walmart_us", claims.tenantId());
        assertTrue(claims.hasScope("read:ads"));
        assertTrue(claims.canAccessCampaign("cmp_1"));
        assertFalse(claims.canAccessCampaign("cmp_other"));
    }

    @Test
    void rejectsTamperedPayload() throws Exception {
        KeyPair kp = generateEd25519();
        String token = mint("{\"tenant_id\":\"a\",\"exp\":\"%s\"}"
                .formatted(Instant.now().plusSeconds(300)), kp);

        // Flip a character in the body segment.
        String[] parts = token.split("\\.");
        parts[2] = parts[2].substring(0, parts[2].length() - 2)
                + (parts[2].endsWith("A") ? "B" : "A");
        String tampered = String.join(".", parts);

        PasetoV4PublicVerifier verifier = verifier(kp, null, null);
        assertThrows(PasetoException.class, () -> verifier.verify(tampered));
    }

    @Test
    void rejectsExpiredToken() throws Exception {
        KeyPair kp = generateEd25519();
        String token = mint("{\"tenant_id\":\"a\",\"exp\":\"%s\"}"
                .formatted(Instant.now().minusSeconds(3600)), kp);

        PasetoV4PublicVerifier verifier = verifier(kp, null, null);
        assertThrows(PasetoException.class, () -> verifier.verify(token));
    }

    @Test
    void rejectsWrongKey() throws Exception {
        KeyPair signing = generateEd25519();
        KeyPair other = generateEd25519();
        String token = mint("{\"tenant_id\":\"a\",\"exp\":\"%s\"}"
                .formatted(Instant.now().plusSeconds(300)), signing);

        PasetoV4PublicVerifier verifier = verifier(other, null, null);
        assertThrows(PasetoException.class, () -> verifier.verify(token));
    }

    @Test
    void rejectsWrongIssuer() throws Exception {
        KeyPair kp = generateEd25519();
        String token = mint("{\"tenant_id\":\"a\",\"iss\":\"evil\",\"exp\":\"%s\"}"
                .formatted(Instant.now().plusSeconds(300)), kp);

        PasetoV4PublicVerifier verifier = verifier(kp, "auth", null);
        assertThrows(PasetoException.class, () -> verifier.verify(token));
    }

    // ---- helpers ----

    private static KeyPair generateEd25519() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static PasetoV4PublicVerifier verifier(KeyPair kp, String iss, String aud) {
        String pub = Base64.getEncoder().encodeToString(kp.getPublic().getEncoded()); // X.509 DER
        return new PasetoV4PublicVerifier(pub, iss, aud, Duration.ofSeconds(0));
    }

    /** Build a signed v4.public token (no footer / implicit). */
    private static String mint(String payloadJson, KeyPair kp) throws Exception {
        byte[] m = payloadJson.getBytes(StandardCharsets.UTF_8);
        byte[] m2 = pae(HEADER.getBytes(StandardCharsets.UTF_8), m, new byte[0], new byte[0]);

        Signature ed = Signature.getInstance("Ed25519");
        ed.initSign(kp.getPrivate());
        ed.update(m2);
        byte[] sig = ed.sign();

        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(m);
        body.write(sig);
        return HEADER + URL.encodeToString(body.toByteArray());
    }

    private static byte[] le64(long n) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (n & 0xFF);
            n >>>= 8;
        }
        out[7] &= 0x7F;
        return out;
    }

    private static byte[] pae(byte[]... pieces) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(le64(pieces.length));
        for (byte[] p : pieces) {
            out.write(le64(p.length));
            out.write(p);
        }
        return out.toByteArray();
    }
}
