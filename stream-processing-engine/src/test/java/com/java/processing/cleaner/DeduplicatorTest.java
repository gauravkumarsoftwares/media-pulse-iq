package com.java.processing.cleaner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the deduplication filter and its TTL eviction.
 */
class DeduplicatorTest {

    @Test
    @DisplayName("Unique then duplicate within TTL")
    void detectsDuplicates() {
        Deduplicator dedup = new Deduplicator(Duration.ofMillis(1000L));
        assertTrue(dedup.isUnique("e1", 0));
        assertFalse(dedup.isUnique("e1", 100));
        assertEquals(1, dedup.size());
    }

    @Test
    @DisplayName("Evicts entries older than TTL")
    void evictsExpired() {
        Deduplicator dedup = new Deduplicator(Duration.ofMillis(1000L));
        assertTrue(dedup.isUnique("e1", 0));
        assertTrue(dedup.isUnique("e2", 1500)); // triggers eviction of e1
        assertTrue(dedup.isUnique("e1", 1500)); // e1 treated as new again
        assertEquals(2, dedup.size());
    }
}
