package com.java.query.service;

import com.java.query.dto.TimeSeriesPoint;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Read-side query abstraction (Interface Segregation, architecture 5).
 */
public interface QueryService {

    /**
     * Default hot-window look-back when no {@code from} is supplied (A3 — DRY).
     * Single source of truth shared by {@link TieredInsightsEngine} and
     * {@link InsightsServiceImpl}.
     */
    Duration DEFAULT_HOT_WINDOW = Duration.ofHours(2);

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
     * Time-bounded overload that enables tier routing (5.2).
     *
     * @param from  start of the query window; {@code null} implies the hot window
     */
    default long getCampaignCount(String tenantId, String campaignId,
                                   String metricType, Instant from) {
        return getCampaignCount(tenantId, campaignId, metricType);
    }

    /**
     * Return a bucketed time-series of counts for the given metric.
     * Each {@link TimeSeriesPoint} represents one grain bucket between {@code from} and {@code to}.
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
     * @return ordered list of bucketed data points
     */
    default List<TimeSeriesPoint> getTimeSeries(String tenantId, String campaignId,
                                                 String metricType,
                                                 Instant from, Instant to,
                                                 String grain, String placement) {
        long total = getCampaignCount(tenantId, campaignId, metricType, from);
        return List.of(new TimeSeriesPoint(
                (to != null ? to : Instant.now()).toString(), total));
    }
}
