package com.java.query.reconciliation;

import com.java.model.redis.RedisKeySchema;

/**
 * Immutable value object identifying a single campaign metric dimension
 * (tenantId × campaignId × eventType).
 *
 * <p>Key building is delegated to {@link RedisKeySchema} (D3 DRY fix).
 */
public record CampaignKey(String tenantId, String campaignId, String eventType) {

    /** Redis aggregate hash key: {@code campaign:{tenantId}:{campaignId}} */
    public String redisHashKey() {
        return RedisKeySchema.hashKey(tenantId, campaignId);
    }

    /** Redis aggregate hash field: the event type string (CLICK, IMPRESSION, …) */
    public String redisHashField() {
        return eventType;
    }

    @Override
    public String toString() {
        return tenantId + "/" + campaignId + "/" + eventType;
    }
}

