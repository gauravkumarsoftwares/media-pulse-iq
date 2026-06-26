package com.java.security.pii;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.regex.Pattern;

/**
 * Utilities to keep personally identifiable information (PII) out of logs and
 * low-trust sinks (OWASP A09). Use {@link #mask(String)} on any free-form text
 * that may contain emails/IPs before logging, and {@link #pseudonymize(String)}
 * to replace a direct identifier (e.g. user id) with a stable, non-reversible
 * token suitable for correlation without exposing the raw value.
 */
public final class PiiMasker {

    private static final Pattern EMAIL =
            Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");
    private static final Pattern IPV4 =
            Pattern.compile("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b");

    private PiiMasker() {
    }

    /** Mask emails and IPv4 addresses in arbitrary text. Null-safe. */
    public static String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String masked = EMAIL.matcher(text).replaceAll("***@***");
        masked = IPV4.matcher(masked).replaceAll("***.***.***.***");
        return masked;
    }

    /**
     * Stable, non-reversible pseudonym for a direct identifier: returns
     * {@code "anon_" + first 12 hex of SHA-256(value)}. Suitable for triage and
     * correlation in the DLQ / logs without persisting the raw identifier.
     */
    public static String pseudonymize(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder("anon_");
            for (int i = 0; i < 6; i++) {
                hex.append(String.format("%02x", digest[i]));
            }
            return hex.toString();
        } catch (Exception ex) {
            // Never fall back to the raw value.
            return "anon_unknown";
        }
    }
}
