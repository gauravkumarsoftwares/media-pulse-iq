package com.java.query.service;

import com.java.query.store.StarTreeStore;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Legacy in-memory CQRS query engine backed by the local {@link StarTreeStore}.
 *
 * <p>Kept as a non-primary {@link QueryService} implementation for unit tests and
 * local-only deployments without Redis or Pinot. The production primary bean is
 * {@link TieredInsightsEngine} (marked {@code @Primary}).
 */
@Service
@RequiredArgsConstructor
public final class InsightsQueryEngine implements QueryService {

    private final StarTreeStore store;

    @Override
    public long getCampaignCount(String tenantId, String campaignId, String metricType) {
        return store.count(tenantId, campaignId, metricType);
    }
}
