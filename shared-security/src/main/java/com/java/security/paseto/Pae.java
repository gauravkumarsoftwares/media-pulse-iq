package com.java.security.paseto;

import java.io.ByteArrayOutputStream;

/**
 * PASETO Pre-Authentication Encoding (PAE), per the PASETO spec.
 *
 * <p>{@code PAE(pieces) = LE64(count) || ( LE64(len(piece)) || piece )*}
 * where {@code LE64} is a 64-bit unsigned little-endian length with the most
 * significant bit cleared. PAE makes the signed/encrypted pre-image
 * unambiguous, preventing canonicalization attacks.
 */
final class Pae {

    private Pae() {
    }

    /** 64-bit little-endian length with the top bit cleared (spec requirement). */
    static byte[] le64(long n) {
        byte[] out = new byte[8];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (n & 0xFF);
            n >>>= 8;
        }
        // Clear the MSB to keep the value unambiguously non-negative.
        out[7] &= 0x7F;
        return out;
    }

    /** Encode an ordered list of byte pieces using PAE. */
    static byte[] encode(byte[]... pieces) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        writeAll(buffer, le64(pieces.length));
        for (byte[] piece : pieces) {
            writeAll(buffer, le64(piece.length));
            writeAll(buffer, piece);
        }
        return buffer.toByteArray();
    }

    private static void writeAll(ByteArrayOutputStream buffer, byte[] bytes) {
        buffer.write(bytes, 0, bytes.length);
    }
}
