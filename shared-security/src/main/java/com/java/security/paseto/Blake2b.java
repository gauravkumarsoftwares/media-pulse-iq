package com.java.security.paseto;

import org.bouncycastle.crypto.digests.Blake2bDigest;

/**
 * Keyed BLAKE2b (the {@code crypto_generichash} primitive used by PASETO
 * {@code v4.local}) on top of BouncyCastle. Used both to derive the
 * encryption/auth keys and to compute the authentication tag.
 */
final class Blake2b {

    private Blake2b() {
    }

    /**
     * @param key         the BLAKE2b key
     * @param message     the message to hash
     * @param outLenBytes desired digest length, in bytes (1..64)
     * @return the keyed digest
     */
    static byte[] mac(byte[] key, byte[] message, int outLenBytes) {
        // NB: this 4-arg constructor takes the digest length in BYTES.
        Blake2bDigest digest = new Blake2bDigest(key, outLenBytes, null, null);
        digest.update(message, 0, message.length);
        byte[] out = new byte[outLenBytes];
        digest.doFinal(out, 0);
        return out;
    }
}

