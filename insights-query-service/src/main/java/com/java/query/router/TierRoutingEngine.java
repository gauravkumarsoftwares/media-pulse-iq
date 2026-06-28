package com.java.query.router;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Time-boundary routing engine for the hybrid read path (architecture 5.2).
 *
 * <p>Tier boundaries are now externalised via {@link TierRoutingProperties}
 * (OCP fix — B2): changing {@code platform.query.routing.hot-window-hours}
 * or {@code warm-window-days} requires no recompile.
 *
 * <ul>
 *   <li>within hot window  → {@link QueryTier#REDIS_CACHE}</li>
 *   <li>within warm window → {@link QueryTier#PINOT_OLAP}</li>
 *   <li>beyond warm window → {@link QueryTier#TRINO_LAKEHOUSE}</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public final class TierRoutingEngine {

    private final TierRoutingProperties props;

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
        if (age.compareTo(props.hotWindow()) <= 0) {
            return QueryTier.REDIS_CACHE;
        }
        if (age.compareTo(props.warmWindow()) <= 0) {
            return QueryTier.PINOT_OLAP;
        }
        return QueryTier.TRINO_LAKEHOUSE;
    }
}
