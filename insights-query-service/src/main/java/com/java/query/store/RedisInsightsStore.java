package com.java.query.store;

import com.java.model.redis.RedisKeySchema;
import com.java.query.dto.TimeSeriesPoint;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Redis-backed hot-tier read/write store for campaign metrics (architecture 4).
 *
 * <h3>Key schema</h3>
 * <pre>
 *   Aggregate hash : campaign:{tenantId}:{campaignId}  → field={eventType}, value=total count
 *   Time-series hash: ts:{tenantId}:{campaignId}:{eventType} → field={hourBucket_ms}, value=count
 * </pre>
 *
 * <p>Key building is delegated to {@link RedisKeySchema} (shared-model) so the
 * Flink sink and this store always use the same key format (DRY — D3).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RedisInsightsStore {

    private final StringRedisTemplate redisTemplate;

    // ---- Aggregate counter (HGET) -------------------------------------------

    /**
     * Retrieve the aggregated count for a campaign metric from the Redis hash.
     *
     * @return {@link Optional} containing the count, or empty on cache miss / error
     */
    public Optional<Long> getCount(String tenantId, String campaignId, String metricType) {
        String key = RedisKeySchema.hashKey(tenantId, campaignId);
        try {
            Object raw = redisTemplate.opsForHash().get(key, metricType);
            if (raw == null) return Optional.empty();
            return Optional.of(Long.parseLong(raw.toString()));
        } catch (Exception ex) {
            log.warn("Redis HGET failed for key={} field={}: {}", key, metricType, ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Atomically increment the aggregate counter for a campaign metric (B3 — DIP fix).
     * Used by the hourly reconciliation job to auto-patch Redis deficits without
     * requiring a direct {@code StringRedisTemplate} dependency in the job.
     *
     * @param delta positive value to add (typically the deficit vs Pinot)
     */
    public void incrementCount(String tenantId, String campaignId,
                                String metricType, long delta) {
        String key = RedisKeySchema.hashKey(tenantId, campaignId);
        try {
            redisTemplate.opsForHash().increment(key, metricType, delta);
            log.debug("[redis] incrementCount tenant={} campaign={} metric={} delta={}",
                    tenantId, campaignId, metricType, delta);
        } catch (Exception ex) {
            log.warn("Redis HINCRBY patch failed key={} field={}: {}", key, metricType, ex.getMessage());
            throw new RuntimeException("Redis increment failed for " + key + "/" + metricType, ex);
        }
    }

    // ---- Time-series (HGETALL → filter → sort) -------------------------------

    /**
     * Return grain-bucketed time-series data from the Redis time-series hash
     * (REDIS-TS fix — replaces even-distribution approximation).
     *
     * <p>The time-series hash is written by Flink's {@code RedisHotCounterSink}
     * using {@code HINCRBY ts:{tenantId}:{campaignId}:{eventType} {hourBucket_ms} 1}.
     * This method reads all fields, filters to the requested window, and sorts by bucket.
     *
     * @param from       window start (inclusive)
     * @param to         window end   (inclusive)
     * @param metricType event type (CLICK, IMPRESSION, …)
     * @return ordered list of {@link TimeSeriesPoint}; empty on cache miss or error
     */
    public List<TimeSeriesPoint> getTimeSeries(String tenantId, String campaignId,
                                                String metricType,
                                                Instant from, Instant to) {
        String key = RedisKeySchema.timeSeriesKey(tenantId, campaignId, metricType);
        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
            if (entries.isEmpty()) return Collections.emptyList();

            long fromMs = from.toEpochMilli();
            long toMs   = to.toEpochMilli();

            return entries.entrySet().stream()
                    .map(e -> Map.entry(
                            Long.parseLong(e.getKey().toString()),
                            Long.parseLong(e.getValue().toString())))
                    .filter(e -> e.getKey() >= fromMs && e.getKey() <= toMs)
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> new TimeSeriesPoint(
                            Instant.ofEpochMilli(e.getKey()).toString(), e.getValue()))
                    .collect(Collectors.toList());
        } catch (Exception ex) {
            log.warn("Redis HGETALL timeseries failed key={}: {}", key, ex.getMessage());
            return Collections.emptyList();
        }
    }

    // ---- Health / debug ------------------------------------------------------

    /** Check whether a campaign has any counter data in the hot tier. */
    public boolean exists(String tenantId, String campaignId) {
        try {
            return Boolean.TRUE.equals(
                    redisTemplate.hasKey(RedisKeySchema.hashKey(tenantId, campaignId)));
        } catch (Exception ex) {
            return false;
        }
    }
}
