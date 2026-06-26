package com.java.security.paseto;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

/**
 * PASETO {@code v4.local} symmetric tokens (encrypt-then-MAC).
 *
 * <p>Per the PASETO spec, {@code v4.local} uses:
 * <ul>
 *   <li><b>XChaCha20</b> (192-bit nonce) for encryption,</li>
 *   <li><b>keyed BLAKE2b</b> for key derivation and the authentication tag,</li>
 *   <li><b>PAE</b> ({@link Pae}) over {@code (header, nonce, ciphertext, footer,
 *       implicit)} as the MAC pre-image.</li>
 * </ul>
 *
 * <p>This is the internal token format for the <em>token-exchange</em> design
 * (Option C): the edge gateway verifies the external {@code v4.public} token and
 * re-mints a short-lived {@code v4.local} token with a shared symmetric key; the
 * services {@link #decrypt decrypt}/verify it. The external token never crosses
 * the trust boundary.
 */
public final class PasetoV4Local {

    private static final String HEADER = "v4.local.";
    private static final byte[] HEADER_BYTES = HEADER.getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFO_ENCRYPTION_KEY = "paseto-encryption-key".getBytes(StandardCharsets.UTF_8);
    private static final byte[] INFO_AUTH_KEY = "paseto-auth-key-for-aead".getBytes(StandardCharsets.UTF_8);

    private static final int KEY_LEN = 32;
    private static final int NONCE_LEN = 32;     // random pre-nonce
    private static final int SUBKEY_LEN = 56;    // 32-byte Ek || 24-byte n2
    private static final int XNONCE_LEN = 24;    // XChaCha20 nonce
    private static final int MAC_LEN = 32;

    private static final SecureRandom RNG = new SecureRandom();
    private static final Base64.Encoder URL = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder URL_DECODER = Base64.getUrlDecoder();

    private PasetoV4Local() {
    }

    /** Encrypt with no footer / implicit assertion. */
    public static String encrypt(byte[] key, byte[] message) {
        return encrypt(key, message, null, null);
    }

    /**
     * Encrypt a payload into a {@code v4.local} token.
     *
     * @param key      32-byte symmetric key
     * @param message  the cleartext payload (typically JSON claims)
     * @param footer   optional, authenticated-but-not-encrypted footer (e.g. {@code kid})
     * @param implicit optional implicit assertion (channel/request binding)
     */
    public static String encrypt(byte[] key, byte[] message, byte[] footer, byte[] implicit) {
        requireKey(key);
        byte[] f = footer == null ? new byte[0] : footer;
        byte[] i = implicit == null ? new byte[0] : implicit;

        byte[] nonce = new byte[NONCE_LEN];
        RNG.nextBytes(nonce);

        byte[] tmp = Blake2b.mac(key, concat(INFO_ENCRYPTION_KEY, nonce), SUBKEY_LEN);
        byte[] ek = Arrays.copyOfRange(tmp, 0, KEY_LEN);
        byte[] n2 = Arrays.copyOfRange(tmp, KEY_LEN, SUBKEY_LEN);
        byte[] ak = Blake2b.mac(key, concat(INFO_AUTH_KEY, nonce), MAC_LEN);

        byte[] ciphertext = xchacha20(ek, n2, message);
        byte[] preAuth = Pae.encode(HEADER_BYTES, nonce, ciphertext, f, i);
        byte[] tag = Blake2b.mac(ak, preAuth, MAC_LEN);

        String token = HEADER + URL.encodeToString(concat(nonce, ciphertext, tag));
        if (f.length > 0) {
            token += "." + URL.encodeToString(f);
        }
        return token;
    }

    /** Decrypt with no implicit assertion. */
    public static byte[] decrypt(byte[] key, String token) {
        return decrypt(key, token, null);
    }

    /**
     * Decrypt + authenticate a {@code v4.local} token, returning the cleartext
     * payload.
     *
     * @throws PasetoException if the token is malformed or the authentication tag
     *                         does not match (tampering, wrong key, etc.)
     */
    public static byte[] decrypt(byte[] key, String token, byte[] implicit) {
        requireKey(key);
        if (token == null || token.isBlank()) {
            throw new PasetoException("Missing token");
        }
        String raw = token.startsWith("Bearer ") ? token.substring(7).trim() : token.trim();
        if (!raw.startsWith(HEADER)) {
            throw new PasetoException("Unsupported PASETO version/purpose (expected v4.local)");
        }

        String[] parts = raw.split("\\.");
        if (parts.length != 3 && parts.length != 4) {
            throw new PasetoException("Malformed PASETO token");
        }

        byte[] body = decode(parts[2]);
        byte[] footer = parts.length == 4 ? decode(parts[3]) : new byte[0];
        byte[] i = implicit == null ? new byte[0] : implicit;
        if (body.length < NONCE_LEN + MAC_LEN) {
            throw new PasetoException("Token body shorter than nonce + tag");
        }

        byte[] nonce = Arrays.copyOfRange(body, 0, NONCE_LEN);
        byte[] ciphertext = Arrays.copyOfRange(body, NONCE_LEN, body.length - MAC_LEN);
        byte[] tag = Arrays.copyOfRange(body, body.length - MAC_LEN, body.length);

        byte[] tmp = Blake2b.mac(key, concat(INFO_ENCRYPTION_KEY, nonce), SUBKEY_LEN);
        byte[] ek = Arrays.copyOfRange(tmp, 0, KEY_LEN);
        byte[] n2 = Arrays.copyOfRange(tmp, KEY_LEN, SUBKEY_LEN);
        byte[] ak = Blake2b.mac(key, concat(INFO_AUTH_KEY, nonce), MAC_LEN);

        byte[] preAuth = Pae.encode(HEADER_BYTES, nonce, ciphertext, footer, i);
        byte[] expected = Blake2b.mac(ak, preAuth, MAC_LEN);
        if (!MessageDigest.isEqual(expected, tag)) {  // constant-time compare
            throw new PasetoException("Invalid token authentication tag");
        }

        // XChaCha20 is a symmetric stream cipher: the same op decrypts.
        return xchacha20(ek, n2, ciphertext);
    }

    private static byte[] xchacha20(byte[] key, byte[] nonce24, byte[] input) {
        if (nonce24.length != XNONCE_LEN) {
            throw new PasetoException("Internal error: bad XChaCha20 nonce length");
        }
        return XChaCha20.process(key, nonce24, input);
    }

    private static void requireKey(byte[] key) {
        if (key == null || key.length != KEY_LEN) {
            throw new PasetoException("v4.local key must be exactly 32 bytes");
        }
    }

    private static byte[] decode(String b64Url) {
        try {
            return URL_DECODER.decode(b64Url);
        } catch (IllegalArgumentException ex) {
            throw new PasetoException("Invalid base64url segment", ex);
        }
    }

    private static byte[] concat(byte[]... arrays) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] a : arrays) {
            out.write(a, 0, a.length);
        }
        return out.toByteArray();
    }
}



