package com.java.security.paseto;

import java.util.Base64;

/**
 * Parses a 32-byte symmetric key (for PASETO {@code v4.local}) from hex (64
 * chars) or Base64 (URL or standard), auto-detected.
 */
public final class SymmetricKeys {

    private static final int KEY_LEN = 32;

    private SymmetricKeys() {
    }

    public static byte[] parse32(String material) {
        if (material == null || material.isBlank()) {
            throw new PasetoException("Empty v4.local symmetric key");
        }
        byte[] key = decodeFlexible(material.trim());
        if (key.length != KEY_LEN) {
            throw new PasetoException("v4.local key must decode to 32 bytes, got " + key.length);
        }
        return key;
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
        try {
            return Base64.getDecoder().decode(b64);
        } catch (IllegalArgumentException ex) {
            throw new PasetoException("Unable to decode v4.local key (expected hex or base64)", ex);
        }
    }

    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }
}

