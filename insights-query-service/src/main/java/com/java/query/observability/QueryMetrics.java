package com.java.query.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Micrometer metrics for the insights-query-service read path (Phase 6 Observability).
 *
 * <h2>Metrics emitted</h2>
 * <pre>
 *  ads.queries.total{tenantId,metricType,tier}     – Counter  – queries served, by serving tier
 *  ads.queries.latency{tier,metricType}             – Timer    – per-query latency by tier
 *  ads.queries.cache.hits{tenantId}                 – Counter  – Redis hot-cache hits
 *  ads.queries.cache.misses{tenantId}               – Counter  – Redis hot-cache misses (fall-through)
 * </pre>
 *
 * <p>The {@code tier} tag values match {@link com.java.query.router.QueryTier}:
 * {@code redis_cache}, {@code pinot_olap}, {@code trino_lakehouse}.
 */
@Component
@RequiredArgsConstructor
public class QueryMetrics {

    private final MeterRegistry registry;

    public static final String QUERIES_TOTAL   = "ads.queries.total";
    public static final String QUERY_LATENCY   = "ads.queries.latency";
    public static final String CACHE_HITS      = "ads.queries.cache.hits";
    public static final String CACHE_MISSES    = "ads.queries.cache.misses";

    // ---- public API --------------------------------------------------------

    /** Start a latency sample before executing the tiered query. */
    public Timer.Sample startQuery() {
        return Timer.start(registry);
    }

    /**
     * Stop the latency sample and record all query metrics.
     *
     * @param sample     the sample returned by {@link #startQuery()}
     * @param tier       the serving tier used (lowercase name from QueryTier)
     * @param tenantId   tenant scope
     * @param metricType CLICK / IMPRESSION / CLICK_TO_BASKET
     */
    public void stopQuery(Timer.Sample sample, String tier, String tenantId, String metricType) {
        sample.stop(Timer.builder(QUERY_LATENCY)
                .description("Per-query latency by serving tier")
                .tag("tier", tier)
                .tag("metricType", safe(metricType))
                .register(registry));

        counter(QUERIES_TOTAL,
                "tenantId", safe(tenantId),
                "metricType", safe(metricType),
                "tier", tier)
                .increment();
    }

    /** Record a Redis hot-cache hit. */
    public void recordCacheHit(String tenantId) {
        counter(CACHE_HITS, "tenantId", safe(tenantId)).increment();
    }

    /** Record a Redis hot-cache miss (query falls through to Pinot or StarTree). */
    public void recordCacheMiss(String tenantId) {
        counter(CACHE_MISSES, "tenantId", safe(tenantId)).increment();
    }

    // ---- helpers -----------------------------------------------------------

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }

    private static String safe(String v) {
        return (v == null || v.isBlank()) ? "unknown" : v;
    }
}

