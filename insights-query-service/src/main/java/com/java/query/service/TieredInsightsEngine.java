package com.java.query.service;

import com.java.query.observability.QueryMetrics;
import com.java.query.router.QueryTier;
import com.java.query.router.TierRoutingEngine;
import com.java.query.store.PinotRestClient;
import com.java.query.store.RedisInsightsStore;
import com.java.query.store.StarTreeStore;
import com.java.query.store.TrinoIcebergClient;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Production {@link QueryService} routing to the three serving tiers (architecture 5.2).
 *
 * <h3>Tier routing</h3>
 * <pre>
 *   window &lt; 48h  =&gt; REDIS_CACHE     (P99 &lt; 7.5 ms)
 *   window &lt; 30d  =&gt; PINOT_OLAP      (P99 &lt; 85 ms)
 *   window &gt; 30d  =&gt; TRINO_LAKEHOUSE (P99 &lt; 4.2 s, Iceberg/S3)
 * </pre>
 *
 * <h3>Time-series design</h3>
 * <p>For the TRINO_LAKEHOUSE tier, {@link #getTimeSeries} calls
 * {@link TrinoIcebergClient#queryTimeSeries} which executes a native Trino
 * {@code date_trunc + GROUP BY} query on Iceberg Parquet files with partition
 * pruning on {@code event_date} — returning real bucketed counts.
 * Redis and Pinot tiers distribute a scalar aggregate evenly across buckets
 * (approximation; replace the Pinot branch with a dateTimeConvert GROUP BY query
 * for production-exact bucketing).
 */
@Service
@Primary
@RequiredArgsConstructor
@Slf4j
public class TieredInsightsEngine implements QueryService {

    private final TierRoutingEngine tierRoutingEngine;
    private final RedisInsightsStore redisStore;
    private final PinotRestClient pinotClient;
    private final TrinoIcebergClient trinoClient;
    private final StarTreeStore starTreeStore;
    private final QueryMetrics queryMetrics;

    // ---- Scalar aggregate -----------------------------------------------

    @Override
    public long getCampaignCount(String tenantId, String campaignId, String metricType) {
        return getCampaignCount(tenantId, campaignId, metricType, null);
    }

    @Override
    public long getCampaignCount(String tenantId, String campaignId,
                                  String metricType, Instant from) {
        QueryTier tier = tierRoutingEngine.resolveTier(from);
        String tierLabel = tier.name().toLowerCase();
        log.debug("getCampaignCount tenant={} campaign={} metric={} tier={}",
                tenantId, campaignId, metricType, tier);

        Timer.Sample sample = queryMetrics.startQuery();
        try {
            return switch (tier) {
                case REDIS_CACHE -> {
                    Optional<Long> hot = redisStore.getCount(tenantId, campaignId, metricType);
                    if (hot.isPresent()) {
                        queryMetrics.recordCacheHit(tenantId);
                        yield hot.get();
                    }
                    queryMetrics.recordCacheMiss(tenantId);
                    long pinotCount = pinotClient.queryCount(tenantId, campaignId, metricType);
                    yield pinotCount > 0 ? pinotCount
                            : starTreeStore.count(tenantId, campaignId, metricType);
                }
                case PINOT_OLAP -> {
                    long count = pinotClient.queryCount(tenantId, campaignId, metricType);
                    yield count > 0 ? count
                            : starTreeStore.count(tenantId, campaignId, metricType);
                }
                case TRINO_LAKEHOUSE -> {
                    // Always supply time bounds for Iceberg partition pruning.
                    Instant trinoTo   = Instant.now();
                    Instant trinoFrom = (from != null) ? from : trinoTo.minus(31, ChronoUnit.DAYS);
                    long trinoCount = trinoClient.queryCount(tenantId, campaignId, metricType,
                            trinoFrom, trinoTo);
                    yield trinoCount > 0 ? trinoCount
                            : starTreeStore.count(tenantId, campaignId, metricType);
                }
            };
        } finally {
            queryMetrics.stopQuery(sample, tierLabel, tenantId, metricType);
        }
    }

    // ---- Time-series (grain-bucketed) ------------------------------------

    /**
     * Return a bucketed time series between {@code from} and {@code to} at the
     * requested {@code grain} (minute / hour / day).
     *
     * <p>For TRINO_LAKEHOUSE, calls {@link TrinoIcebergClient#queryTimeSeries}
     * which issues a native Trino {@code date_trunc + GROUP BY} — real bucket counts.
     * For Redis and Pinot, distributes a scalar aggregate evenly across buckets.
     */
    @Override
    public List<Map<String, Object>> getTimeSeries(String tenantId, String campaignId,
                                                    String metricType,
                                                    Instant from, Instant to,
                                                    String grain, String placement) {
        Instant effectiveTo   = (to   != null) ? to   : Instant.now();
        Instant effectiveFrom = (from != null) ? from : effectiveTo.minus(Duration.ofHours(2));

        QueryTier tier = tierRoutingEngine.resolveTier(effectiveFrom);
        String tierLabel = tier.name().toLowerCase();
        Timer.Sample sample = queryMetrics.startQuery();

        try {
            return switch (tier) {
                case REDIS_CACHE -> {
                    Optional<Long> hot = redisStore.getCount(tenantId, campaignId, metricType);
                    if (hot.isPresent()) {
                        queryMetrics.recordCacheHit(tenantId);
                    } else {
                        queryMetrics.recordCacheMiss(tenantId);
                    }
                    long total = hot.orElseGet(() -> {
                        long p = pinotClient.queryCount(tenantId, campaignId, metricType);
                        return p > 0 ? p : starTreeStore.count(tenantId, campaignId, metricType);
                    });
                    yield distributeEvenly(effectiveFrom, effectiveTo, grain, total);
                }
                case PINOT_OLAP -> {
                    // TODO: replace with Pinot dateTimeConvert GROUP BY for exact bucketing.
                    long total = pinotClient.queryCount(tenantId, campaignId, metricType);
                    if (total == 0) total = starTreeStore.count(tenantId, campaignId, metricType);
                    yield distributeEvenly(effectiveFrom, effectiveTo, grain, total);
                }
                case TRINO_LAKEHOUSE -> {
                    // Native Trino date_trunc + GROUP BY — real bucketed series from Iceberg.
                    List<Map<String, Object>> series = trinoClient.queryTimeSeries(
                            tenantId, campaignId, metricType, effectiveFrom, effectiveTo, grain);
                    if (!series.isEmpty()) {
                        yield series;
                    }
                    // Trino not yet configured — fall back to StarTree in-memory approximation.
                    long fallback = starTreeStore.count(tenantId, campaignId, metricType);
                    yield distributeEvenly(effectiveFrom, effectiveTo, grain, fallback);
                }
            };
        } finally {
            queryMetrics.stopQuery(sample, tierLabel, tenantId, metricType);
        }
    }

    // ---- helpers -------------------------------------------------------

    /**
     * Distribute {@code total} evenly across grain-aligned buckets between
     * {@code from} and {@code to}.  The last bucket absorbs any remainder.
     */
    private static List<Map<String, Object>> distributeEvenly(Instant from, Instant to,
                                                               String grain, long total) {
        List<Instant> buckets = computeBuckets(from, to, grain);
        if (buckets.isEmpty()) {
            return List.of(Map.of("timestamp", to.toString(), "value", total));
        }
        long perBucket = total / buckets.size();
        long remainder = total % buckets.size();

        List<Map<String, Object>> series = new ArrayList<>(buckets.size());
        for (int i = 0; i < buckets.size(); i++) {
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("timestamp", buckets.get(i).toString());
            point.put("value", perBucket + (i == buckets.size() - 1 ? remainder : 0));
            series.add(point);
        }
        return series;
    }

    private static List<Instant> computeBuckets(Instant from, Instant to, String grain) {
        ChronoUnit unit = switch (grain.toLowerCase()) {
            case "minute" -> ChronoUnit.MINUTES;
            case "day"    -> ChronoUnit.DAYS;
            default       -> ChronoUnit.HOURS;
        };

        List<Instant> buckets = new ArrayList<>();
        ZonedDateTime cursor = from.atZone(ZoneOffset.UTC).truncatedTo(unit);
        ZonedDateTime end    = to.atZone(ZoneOffset.UTC);

        while (!cursor.isAfter(end)) {
            buckets.add(cursor.toInstant());
            cursor = cursor.plus(1, unit);
            if (buckets.size() > 10_000) break;   // safety cap
        }
        return buckets;
    }
}
