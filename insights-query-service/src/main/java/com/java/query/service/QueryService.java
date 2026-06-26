package com.java.query.service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Read-side query abstraction (Interface Segregation, architecture §5).
 */
public interface QueryService {

    /**
     * Return the total aggregated count for a campaign metric.
     *
     * @param tenantId   tenant scope from the validated context
     * @param campaignId campaign identifier
     * @param metricType metric/event type (e.g. CLICK, IMPRESSION, CLICK_TO_BASKET)
     * @return the aggregated count
     */
    long getCampaignCount(String tenantId, String campaignId, String metricType);

    /**
     * Time-bounded overload that enables tier routing (§5.2).
     *
     * @param from  start of the query window; {@code null} implies the hot window
     */
    default long getCampaignCount(String tenantId, String campaignId,
                                   String metricType, Instant from) {
        return getCampaignCount(tenantId, campaignId, metricType);
    }

    /**
     * Return a bucketed time-series of counts for the given metric.
     * Each entry in the list is a {@code {timestamp, value}} map representing
     * one grain bucket between {@code from} and {@code to}.
     *
     * <p>Default implementation returns a single aggregate point (compatible
     * with implementations that don't support time-series bucketing).
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType CLICK / IMPRESSION / CLICK_TO_BASKET
     * @param from       window start (inclusive); null = hot window start
     * @param to         window end (inclusive); null = now
     * @param grain      bucket granularity: {@code minute}, {@code hour}, {@code day}
     * @param placement  optional placement filter (null = all placements)
     * @return ordered list of {timestamp, value} buckets
     */
    default List<Map<String, Object>> getTimeSeries(String tenantId, String campaignId,
                                                     String metricType,
                                                     Instant from, Instant to,
                                                     String grain, String placement) {
        long total = getCampaignCount(tenantId, campaignId, metricType, from);
        return List.of(Map.of("timestamp", (to != null ? to : Instant.now()).toString(),
                "value", total));
    }
}

