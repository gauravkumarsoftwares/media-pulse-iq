package com.java.security.paseto;

import org.bouncycastle.crypto.engines.ChaCha7539Engine;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

/**
 * XChaCha20 stream cipher (the encryption primitive of PASETO {@code v4.local}).
 *
 * <p>BouncyCastle ships ChaCha20 (RFC 7539, 96-bit nonce) but not XChaCha20
 * (192-bit nonce). XChaCha20 is defined as:
 * <ol>
 *   <li><b>HChaCha20</b>(key, nonce[0..16]) → a 32-byte subkey, and</li>
 *   <li><b>ChaCha20</b>(subkey, {@code 0x00000000 || nonce[16..24]}, counter=0)
 *       over the message.</li>
 * </ol>
 * HChaCha20 is implemented here directly; the ChaCha20 stream uses BC's
 * {@link ChaCha7539Engine}. As a stream cipher the same operation encrypts and
 * decrypts.
 */
final class XChaCha20 {

    private static final int[] SIGMA = {0x61707865, 0x3320646e, 0x79622d32, 0x6b206574};

    private XChaCha20() {
    }

    /** Encrypt/decrypt {@code input} with a 32-byte key and 24-byte nonce. */
    static byte[] process(byte[] key, byte[] nonce24, byte[] input) {
        byte[] subkey = hchacha20(key, nonce24);
        byte[] chachaNonce = new byte[12];                 // 0x00000000 || nonce[16..24]
        System.arraycopy(nonce24, 16, chachaNonce, 4, 8);

        ChaCha7539Engine engine = new ChaCha7539Engine();
        engine.init(true, new ParametersWithIV(new KeyParameter(subkey), chachaNonce));
        byte[] out = new byte[input.length];
        engine.processBytes(input, 0, input.length, out, 0);
        return out;
    }

    /** HChaCha20: derive a 32-byte subkey from key + the first 16 nonce bytes. */
    private static byte[] hchacha20(byte[] key, byte[] nonce24) {
        int[] s = new int[16];
        s[0] = SIGMA[0];
        s[1] = SIGMA[1];
        s[2] = SIGMA[2];
        s[3] = SIGMA[3];
        for (int i = 0; i < 8; i++) {
            s[4 + i] = le32(key, i * 4);
        }
        for (int i = 0; i < 4; i++) {
            s[12 + i] = le32(nonce24, i * 4);
        }

        for (int i = 0; i < 10; i++) {       // 20 rounds = 10 double-rounds
            quarterRound(s, 0, 4, 8, 12);
            quarterRound(s, 1, 5, 9, 13);
            quarterRound(s, 2, 6, 10, 14);
            quarterRound(s, 3, 7, 11, 15);
            quarterRound(s, 0, 5, 10, 15);
            quarterRound(s, 1, 6, 11, 12);
            quarterRound(s, 2, 7, 8, 13);
            quarterRound(s, 3, 4, 9, 14);
        }

        byte[] out = new byte[32];           // words 0..3 || 12..15, no final add
        for (int i = 0; i < 4; i++) {
            putLe32(out, i * 4, s[i]);
        }
        for (int i = 0; i < 4; i++) {
            putLe32(out, 16 + i * 4, s[12 + i]);
        }
        return out;
    }

    private static void quarterRound(int[] s, int a, int b, int c, int d) {
        s[a] += s[b];
        s[d] = rotl(s[d] ^ s[a], 16);
        s[c] += s[d];
        s[b] = rotl(s[b] ^ s[c], 12);
        s[a] += s[b];
        s[d] = rotl(s[d] ^ s[a], 8);
        s[c] += s[d];
        s[b] = rotl(s[b] ^ s[c], 7);
    }

    private static int rotl(int v, int n) {
        return (v << n) | (v >>> (32 - n));
    }

    private static int le32(byte[] b, int o) {
        return (b[o] & 0xff)
                | ((b[o + 1] & 0xff) << 8)
                | ((b[o + 2] & 0xff) << 16)
                | ((b[o + 3] & 0xff) << 24);
    }

    private static void putLe32(byte[] b, int o, int v) {
        b[o] = (byte) v;
        b[o + 1] = (byte) (v >>> 8);
        b[o + 2] = (byte) (v >>> 16);
        b[o + 3] = (byte) (v >>> 24);
    }
}

