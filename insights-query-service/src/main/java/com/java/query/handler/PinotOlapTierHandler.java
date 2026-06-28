package com.java.query.handler;

import com.java.query.dto.TimeSeriesPoint;
import com.java.query.service.TimeSeriesBucketUtils;
import com.java.query.store.PinotRestClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Warm-tier handler: reads aggregated counts from Apache Pinot.
 *
 * <p>{@code resolveTimeSeries} now issues a native Pinot
 * {@code dateTimeConvert + GROUP BY} query for exact per-bucket counts
 * (PINOT-TS fix). Even-distribution is retained only as a graceful degradation
 * path when Pinot is disabled or returns an empty result.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PinotOlapTierHandler implements TierQueryHandler {

    private final PinotRestClient         pinotClient;
    private final StarTreeFallbackResolver fallback;
    private final TimeSeriesBucketUtils   bucketUtils;

    @Override
    public long resolveCount(String tenantId, String campaignId,
                             String metricType, Instant from) {
        long count = pinotClient.queryCount(tenantId, campaignId, metricType);
        log.debug("[pinot] queryCount tenant={} campaign={} metric={} → {}",
                tenantId, campaignId, metricType, count);
        return fallback.withFallback(count, tenantId, campaignId, metricType);
    }

    @Override
    public List<TimeSeriesPoint> resolveTimeSeries(String tenantId, String campaignId,
                                                    String metricType,
                                                    Instant from, Instant to, String grain) {
        // Attempt real Pinot dateTimeConvert GROUP BY first.
        List<TimeSeriesPoint> series =
                pinotClient.queryTimeSeries(tenantId, campaignId, metricType, from, to, grain);

        if (!series.isEmpty()) {
            return series;
        }

        // Graceful degradation: Pinot disabled or no data → even distribution.
        log.debug("[pinot] queryTimeSeries empty → distributeEvenly fallback "
                + "tenant={} campaign={}", tenantId, campaignId);
        long total = fallback.withFallback(
                pinotClient.queryCount(tenantId, campaignId, metricType),
                tenantId, campaignId, metricType);
        return bucketUtils.distributeEvenly(from, to, grain, total);
    }
}
