package com.java.query.service;

import com.java.query.dto.TimeSeriesPoint;
import com.java.query.store.StarTreeStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

/**
 * Legacy in-memory CQRS query engine backed by the local {@link StarTreeStore}.
 *
 * <p>Retained as a non-primary {@link QueryService} for unit tests and
 * local-only deployments without Redis, Pinot, or Trino. The production
 * primary bean is {@link TieredInsightsEngine} (marked {@code @Primary}).
 *
 * <p>{@code getTimeSeries} now has a proper override (D1 modularity fix):
 * it uses {@link TimeSeriesBucketUtils#distributeEvenly} to produce grain-bucketed
 * output consistent with what the tiered engine returns, making local-dev
 * time-series responses structurally identical to production responses.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public final class InsightsQueryEngine implements QueryService {

    private final StarTreeStore       store;
    private final TimeSeriesBucketUtils bucketUtils;

    @Override
    public long getCampaignCount(String tenantId, String campaignId, String metricType) {
        log.debug("[SERVICE] InsightsQueryEngine.getCampaignCount tenant={} campaign={} metric={}",
                tenantId, campaignId, metricType);
        long count = store.count(tenantId, campaignId, metricType);
        log.debug("[SERVICE] InsightsQueryEngine.getCampaignCount result count={}", count);
        return count;
    }

    @Override
    public List<TimeSeriesPoint> getTimeSeries(String tenantId, String campaignId,
                                                String metricType,
                                                Instant from, Instant to,
                                                String grain, String placement) {
        long total = store.count(tenantId, campaignId, metricType);
        Instant effectiveTo   = (to   != null) ? to   : Instant.now();
        Instant effectiveFrom = (from != null) ? from : effectiveTo.minus(DEFAULT_HOT_WINDOW);
        return bucketUtils.distributeEvenly(effectiveFrom, effectiveTo, grain, total);
    }
}
