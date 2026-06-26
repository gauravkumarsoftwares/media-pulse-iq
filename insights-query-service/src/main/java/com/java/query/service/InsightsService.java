package com.java.query.service;

import com.java.query.dto.CampaignMetricResponse;
import com.java.security.paseto.PasetoClaims;

/**
 * Read-path service contract for campaign metric queries (architecture §5).
 *
 * <p>Encapsulates input validation, per-campaign authorization, tier routing,
 * and query execution so the controller stays thin.
 */
public interface InsightsService {

    /**
     * Retrieve aggregated metrics for a campaign.
     *
     * @param tenantId   verified tenant identifier from {@code X-Tenant-Context}
     * @param claims     verified PASETO claims for authorization (may be null in dev)
     * @param campaignId the target campaign
     * @param metricType {@code CLICK}, {@code IMPRESSION}, or {@code CLICK_TO_BASKET}
     * @param from       ISO-8601 window start (null = hot window)
     * @param to         ISO-8601 window end (null = now)
     * @param grain      bucket granularity: {@code minute}, {@code hour}, {@code day}
     * @param placement  optional placement filter (null = all placements)
     * @return typed campaign metric response
     * @throws com.java.query.common.ApiException on validation or authorization failure
     */
    CampaignMetricResponse getMetrics(
            String tenantId,
            PasetoClaims claims,
            String campaignId,
            String metricType,
            String from,
            String to,
            String grain,
            String placement);
}

