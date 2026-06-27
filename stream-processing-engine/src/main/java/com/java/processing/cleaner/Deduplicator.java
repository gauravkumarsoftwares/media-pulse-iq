package com.java.processing.cleaner;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Idempotent stream deduplication filter (architecture 3.1).
 * Backed by a concurrent map with TTL eviction to keep memory bounded —
 * the in-process analogue of Flink's RocksDB keyed state with a configurable TTL.
 */
@Component
public final class Deduplicator {

    private final ConcurrentHashMap<String, Long> seen = new ConcurrentHashMap<>();
    private final long ttlMs;

    /**
     * Spring constructor. Reads the deduplication TTL (minutes) from configuration,
     * defaulting to 60 minutes to match {@code platform.dedup.ttl-minutes}.
     */
    public Deduplicator(@Value("${platform.dedup.ttl-minutes:60}") long ttlMinutes) {
        this(Duration.ofMinutes(ttlMinutes));
    }

    /**
     * Precise-TTL constructor (package-private, primarily for tests).
     *
     * @param ttl the time-to-live for dedup state entries
     */
    Deduplicator(Duration ttl) {
        this.ttlMs = ttl.toMillis();
    }

    /**
     * @return {@code true} if the event id is seen for the first time within the TTL window.
     */
    public boolean isUnique(String eventId, long timestampMs) {
        evictExpired(timestampMs);
        return seen.putIfAbsent(eventId, timestampMs) == null;
    }

    private void evictExpired(long now) {
        seen.entrySet().removeIf(e -> (now - e.getValue()) > ttlMs);
    }

    public int size() {
        return seen.size();
    }
}
