package com.java.query.dto;

import java.util.List;

/**
 * Response envelope for a campaign metric query (clicks / impressions / click-to-basket).
 *
 * @param tenantId        the verified tenant scope
 * @param campaignId      the queried campaign
 * @param metric          event type in lower-case (e.g. {@code click})
 * @param from            window start (ISO-8601)
 * @param to              window end (ISO-8601)
 * @param grain           time bucket granularity ({@code minute}, {@code hour}, {@code day})
 * @param placement       optional placement filter (null when not specified)
 * @param series          ordered list of time-bucketed data points
 * @param total           sum of all values in the series
 * @param dataFreshnessMs approximate data-age in milliseconds (tier-dependent)
 * @param source          the serving tier that answered the query (e.g. {@code redis-hot})
 */
public record CampaignMetricResponse(
        String tenantId,
        String campaignId,
        String metric,
        String from,
        String to,
        String grain,
        String placement,
        List<TimeSeriesPoint> series,
        long total,
        long dataFreshnessMs,
        String source) {}

