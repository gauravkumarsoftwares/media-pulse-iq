package com.java.query.handler;

import com.java.query.store.PinotRestClient;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Shared fallback resolver used by tier handlers when the primary store
 * returns zero (STARTREE fix — B4).
 *
 * <p>The in-memory {@code StarTreeStore} stub has been replaced with
 * {@link PinotRestClient}: when the primary store is unavailable or empty,
 * the resolver queries Pinot directly for the authoritative count.
 *
 * <p>{@code StarTreeStore} is retained for local-dev / test use via
 * {@link com.java.query.service.InsightsQueryEngine} and
 * {@link com.java.query.consumer.AggregateConsumer}.
 */
@Component
@RequiredArgsConstructor
public class StarTreeFallbackResolver {

    private final PinotRestClient pinotClient;

    /**
     * Return {@code primary} if positive; otherwise fall back to a live Pinot query.
     *
     * @param primary    the count from the tier's primary store (0 = unavailable / empty)
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType event type
     * @return the primary count, or the Pinot count as a last-resort fallback
     */
    public long withFallback(long primary, String tenantId,
                             String campaignId, String metricType) {
        return primary > 0
                ? primary
                : pinotClient.queryCount(tenantId, campaignId, metricType);
    }
}
