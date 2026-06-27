package com.java.security.paseto;

import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.NamedParameterSpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/**
 * Parses an Ed25519 public key for PASETO {@code v4.public} verification using
 * only the JDK 21 native EdDSA provider (no BouncyCastle / third-party crypto).
 *
 * <p>Accepts the key in any of these forms (auto-detected):
 * <ul>
 *   <li>PEM ({@code -----BEGIN PUBLIC KEY-----} / X.509 SubjectPublicKeyInfo)</li>
 *   <li>Base64 of the X.509 DER</li>
 *   <li>Raw 32-byte public key as hex (64 chars) or Base64</li>
 * </ul>
 */
public final class Ed25519Keys {

    private static final int RAW_KEY_LEN = 32;

    private Ed25519Keys() {
    }

    public static PublicKey parsePublicKey(String material) {
        if (material == null || material.isBlank()) {
            throw new PasetoException("Empty Ed25519 public key");
        }
        String value = material.trim();
        try {
            if (value.contains("-----BEGIN")) {
                return fromX509(pemBody(value));
            }
            byte[] decoded = decodeFlexible(value);
            if (decoded.length == RAW_KEY_LEN) {
                return fromRaw(decoded);
            }
            // Otherwise assume Base64-encoded X.509 DER.
            return fromX509(decoded);
        } catch (PasetoException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new PasetoException("Unable to parse Ed25519 public key", ex);
        }
    }

    private static PublicKey fromX509(byte[] der) throws Exception {
        KeyFactory kf = KeyFactory.getInstance("Ed25519");
        return kf.generatePublic(new X509EncodedKeySpec(der));
    }

    /**
     * Build an {@link java.security.interfaces.EdECPublicKey} from the 32-byte
     * raw encoding: little-endian {@code y} with the x-coordinate sign in the
     * MSB of the final byte (RFC 8032 5.1.2).
     */
    private static PublicKey fromRaw(byte[] raw) throws Exception {
        byte[] little = raw.clone();
        boolean xOdd = (little[RAW_KEY_LEN - 1] & 0x80) != 0;
        little[RAW_KEY_LEN - 1] &= 0x7F;

        // BigInteger expects big-endian; reverse the little-endian y.
        byte[] big = reverse(little);
        BigInteger y = new BigInteger(1, big);

        EdECPoint point = new EdECPoint(xOdd, y);
        EdECPublicKeySpec spec = new EdECPublicKeySpec(NamedParameterSpec.ED25519, point);
        return KeyFactory.getInstance("Ed25519").generatePublic(spec);
    }

    private static byte[] pemBody(String pem) {
        String body = pem
                .replaceAll("-----BEGIN [^-]+-----", "")
                .replaceAll("-----END [^-]+-----", "")
                .replaceAll("\\s", "");
        return Base64.getDecoder().decode(body);
    }

    /** Try hex first (even length, hex chars), else Base64 (URL or standard). */
    private static byte[] decodeFlexible(String value) {
        String compact = value.replaceAll("\\s", "");
        if (compact.matches("(?i)[0-9a-f]+") && compact.length() % 2 == 0) {
            return hex(compact);
        }
        String b64 = compact.replace('-', '+').replace('_', '/');
        switch (b64.length() % 4) {
            case 2 -> b64 += "==";
            case 3 -> b64 += "=";
            default -> { }
        }
        return Base64.getDecoder().decode(b64);
    }

    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private static byte[] reverse(byte[] in) {
        byte[] out = new byte[in.length];
        for (int i = 0; i < in.length; i++) {
            out[i] = in[in.length - 1 - i];
        }
        return out;
    }
}
