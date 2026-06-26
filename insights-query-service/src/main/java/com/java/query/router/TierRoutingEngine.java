package com.java.query.router;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Time-boundary routing engine for the hybrid read path (architecture §5.2).
 *
 * <p>Selects the serving tier based on how far back the query window reaches:
 * <ul>
 *   <li>&lt; 48 hours → {@link QueryTier#REDIS_CACHE}</li>
 *   <li>&lt; 30 days  → {@link QueryTier#PINOT_OLAP}</li>
 *   <li>otherwise     → {@link QueryTier#TRINO_LAKEHOUSE}</li>
 * </ul>
 *
 * <p>In this reference build all tiers resolve against the same in-memory store;
 * the engine still determines the authoritative {@code source} label so the
 * routing contract is testable and ready to back real stores later.
 */
@Component
public final class TierRoutingEngine {

    private static final Duration HOT_WINDOW = Duration.ofHours(48);
    private static final Duration WARM_WINDOW = Duration.ofDays(30);

    /**
     * Resolve the serving tier for a query whose earliest boundary is {@code from}.
     *
     * @param from the start of the query window; {@code null} implies the hot window
     * @return the selected {@link QueryTier}
     */
    public QueryTier resolveTier(Instant from) {
        if (from == null) {
            return QueryTier.REDIS_CACHE;
        }
        Duration age = Duration.between(from, Instant.now());
        if (age.compareTo(HOT_WINDOW) <= 0) {
            return QueryTier.REDIS_CACHE;
        }
        if (age.compareTo(WARM_WINDOW) <= 0) {
            return QueryTier.PINOT_OLAP;
        }
        return QueryTier.TRINO_LAKEHOUSE;
    }
}

