package com.java.query.reconciliation;

/**
 * Immutable value object identifying a single campaign metric dimension
 * (tenantId × campaignId × eventType).
 *
 * <p>Used as the grouping key throughout the reconciliation pipeline.
 */
public record CampaignKey(String tenantId, String campaignId, String eventType) {

    /** Redis hash key:  {@code campaign:{tenantId}:{campaignId}} */
    public String redisHashKey() {
        return "campaign:" + tenantId + ":" + campaignId;
    }

    /** Redis hash field: the event type string (CLICK, IMPRESSION, …) */
    public String redisHashField() {
        return eventType;
    }

    @Override
    public String toString() {
        return tenantId + "/" + campaignId + "/" + eventType;
    }
}

