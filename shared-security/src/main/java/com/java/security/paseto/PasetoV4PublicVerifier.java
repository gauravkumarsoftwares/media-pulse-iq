package com.java.security.paseto;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;

/**
 * Verifier for PASETO {@code v4.public} tokens (Ed25519 signatures), implemented
 * on the JDK 21 native EdDSA provider.
 *
 * <p>Verification steps (PASETO spec, v4.public):
 * <ol>
 *   <li>Assert the token starts with {@code v4.public.}</li>
 *   <li>Base64url-decode the payload into {@code message || signature(64B)}</li>
 *   <li>Recompute {@code m2 = PAE("v4.public.", message, footer, implicit)}</li>
 *   <li>Verify the Ed25519 signature of {@code m2} with the public key</li>
 *   <li>Parse JSON claims and validate {@code exp}/{@code nbf}/{@code iss}/{@code aud}</li>
 * </ol>
 *
 * <p>This class is immutable and thread-safe.
 */
public final class PasetoV4PublicVerifier implements PasetoVerifier {

    private static final String HEADER = "v4.public.";
    private static final int SIGNATURE_LENGTH = 64;

    private final PublicKey publicKey;
    private final String expectedIssuer;   // nullable -> not enforced
    private final String expectedAudience; // nullable -> not enforced
    private final Duration clockSkew;
    private final ObjectMapper mapper;
    private final Base64.Decoder urlDecoder = Base64.getUrlDecoder();

    public PasetoV4PublicVerifier(String publicKeyMaterial,
                                  String expectedIssuer,
                                  String expectedAudience,
                                  Duration clockSkew) {
        this.publicKey = Ed25519Keys.parsePublicKey(publicKeyMaterial);
        this.expectedIssuer = ClaimsValidator.blankToNull(expectedIssuer);
        this.expectedAudience = ClaimsValidator.blankToNull(expectedAudience);
        this.clockSkew = clockSkew == null ? Duration.ofSeconds(60) : clockSkew;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /** Verify with no footer / implicit assertion. */
    public PasetoClaims verify(String token) {
        return verify(token, null);
    }

    /**
     * Verify a token and return its validated claims.
     *
     * @param token the {@code v4.public.*} token (any {@code Bearer } prefix is stripped)
     * @param implicitAssertion optional implicit assertion bound at signing time
     * @throws PasetoException if the token is malformed, unsigned by the key, or
     *                         fails temporal/issuer/audience checks
     */
    public PasetoClaims verify(String token, byte[] implicitAssertion) {
        if (token == null || token.isBlank()) {
            throw new PasetoException("Missing token");
        }
        String raw = token.startsWith("Bearer ") ? token.substring(7).trim() : token.trim();
        if (!raw.startsWith(HEADER)) {
            throw new PasetoException("Unsupported PASETO version/purpose (expected v4.public)");
        }

        String[] parts = raw.split("\\.");
        // v4 . public . payload [ . footer ]
        if (parts.length != 3 && parts.length != 4) {
            throw new PasetoException("Malformed PASETO token");
        }

        byte[] body = decode(parts[2]);
        if (body.length < SIGNATURE_LENGTH) {
            throw new PasetoException("Token body shorter than signature");
        }
        byte[] message = Arrays.copyOfRange(body, 0, body.length - SIGNATURE_LENGTH);
        byte[] signature = Arrays.copyOfRange(body, body.length - SIGNATURE_LENGTH, body.length);
        byte[] footer = parts.length == 4 ? decode(parts[3]) : new byte[0];
        byte[] implicit = implicitAssertion == null ? new byte[0] : implicitAssertion;

        byte[] m2 = Pae.encode(
                HEADER.getBytes(StandardCharsets.UTF_8),
                message,
                footer,
                implicit);

        if (!ed25519Verify(m2, signature)) {
            throw new PasetoException("Invalid token signature");
        }

        PasetoClaims claims = parseClaims(message);
        ClaimsValidator.validate(claims, expectedIssuer, expectedAudience, clockSkew);
        return claims;
    }

    private boolean ed25519Verify(byte[] data, byte[] signature) {
        try {
            Signature ed = Signature.getInstance("Ed25519");
            ed.initVerify(publicKey);
            ed.update(data);
            return ed.verify(signature);
        } catch (Exception ex) {
            throw new PasetoException("Signature verification error", ex);
        }
    }

    private PasetoClaims parseClaims(byte[] json) {
        try {
            return mapper.readValue(json, PasetoClaims.class);
        } catch (Exception ex) {
            throw new PasetoException("Unable to parse token claims", ex);
        }
    }

    private byte[] decode(String b64Url) {
        try {
            return urlDecoder.decode(b64Url);
        } catch (IllegalArgumentException ex) {
            throw new PasetoException("Invalid base64url segment", ex);
        }
    }
}
