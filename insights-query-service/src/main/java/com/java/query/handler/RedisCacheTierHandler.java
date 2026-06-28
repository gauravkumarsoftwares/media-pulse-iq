package com.java.query.handler;

import com.java.query.dto.TimeSeriesPoint;
import com.java.query.observability.QueryMetrics;
import com.java.query.service.TimeSeriesBucketUtils;
import com.java.query.store.PinotRestClient;
import com.java.query.store.RedisInsightsStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Hot-tier handler: reads from Redis with Pinot + StarTree as a fallback chain.
 *
 * <p>{@code resolveTimeSeries} now uses {@link RedisInsightsStore#getTimeSeries}
 * which performs an {@code HGETALL} on the time-series hash written by the Flink
 * {@code RedisHotCounterSink} (REDIS-TS fix). Even-distribution is retained only
 * as a last-resort degradation path when no time-series data exists in Redis.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RedisCacheTierHandler implements TierQueryHandler {

    private final RedisInsightsStore       redisStore;
    private final PinotRestClient          pinotClient;
    private final StarTreeFallbackResolver fallback;
    private final QueryMetrics             queryMetrics;
    private final TimeSeriesBucketUtils    bucketUtils;

    @Override
    public long resolveCount(String tenantId, String campaignId,
                             String metricType, Instant from) {
        Optional<Long> hot = redisStore.getCount(tenantId, campaignId, metricType);
        if (hot.isPresent()) {
            queryMetrics.recordCacheHit(tenantId);
            log.debug("[redis] cache-hit tenant={} campaign={} metric={}",
                    tenantId, campaignId, metricType);
            return hot.get();
        }
        queryMetrics.recordCacheMiss(tenantId);
        log.debug("[redis] cache-miss → pinot fallback tenant={} campaign={} metric={}",
                tenantId, campaignId, metricType);
        return fallback.withFallback(
                pinotClient.queryCount(tenantId, campaignId, metricType),
                tenantId, campaignId, metricType);
    }

    @Override
    public List<TimeSeriesPoint> resolveTimeSeries(String tenantId, String campaignId,
                                                    String metricType,
                                                    Instant from, Instant to, String grain) {
        // Attempt exact Redis time-series lookup (HGETALL + filter).
        List<TimeSeriesPoint> series =
                redisStore.getTimeSeries(tenantId, campaignId, metricType, from, to);

        if (!series.isEmpty()) {
            queryMetrics.recordCacheHit(tenantId);
            log.debug("[redis] timeseries cache-hit tenant={} campaign={} buckets={}",
                    tenantId, campaignId, series.size());
            return series;
        }

        // Cache miss: fall through to Pinot → even-distribution as last resort.
        queryMetrics.recordCacheMiss(tenantId);
        log.debug("[redis] timeseries cache-miss → distributeEvenly fallback "
                + "tenant={} campaign={}", tenantId, campaignId);
        long total = fallback.withFallback(
                pinotClient.queryCount(tenantId, campaignId, metricType),
                tenantId, campaignId, metricType);
        return bucketUtils.distributeEvenly(from, to, grain, total);
    }
}
