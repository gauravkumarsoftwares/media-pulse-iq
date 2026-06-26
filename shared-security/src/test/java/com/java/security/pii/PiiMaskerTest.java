package com.java.security.pii;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PiiMaskerTest {

    @Test
    void masksEmailAndIp() {
        String in = "user jane.doe@example.com from 192.168.10.5 clicked";
        String out = PiiMasker.mask(in);
        assertFalse(out.contains("jane.doe@example.com"));
        assertFalse(out.contains("192.168.10.5"));
        assertTrue(out.contains("***@***"));
        assertTrue(out.contains("***.***.***.***"));
    }

    @Test
    void pseudonymizeIsStableAndNonReversible() {
        String a = PiiMasker.pseudonymize("user-123");
        String b = PiiMasker.pseudonymize("user-123");
        assertEquals(a, b);                     // stable
        assertTrue(a.startsWith("anon_"));
        assertFalse(a.contains("user-123"));    // not reversible / no raw value
    }

    @Test
    void nullSafe() {
        assertEquals(null, PiiMasker.mask(null));
        assertEquals(null, PiiMasker.pseudonymize(null));
    }
}
