package com.java.query.store;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Redis-backed hot-tier read store for campaign metrics (architecture 4).
 *
 * <p>Hot counters are written by the Flink {@code RedisHotCounterSink} via atomic
 * {@code HINCRBY} operations. This component reads them with {@code HGET}.
 *
 * <p><strong>Key schema:</strong> {@code campaign:{tenantId}:{campaignId}}
 * <br><strong>Hash field:</strong> event type string (CLICK, IMPRESSION, CLICK_TO_BASKET …)
 *
 * <p>Returns {@link Optional#empty()} when the key / field does not exist (cache miss),
 * allowing the tier router to fall through to Pinot.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RedisInsightsStore {

    private final StringRedisTemplate redisTemplate;

    /**
     * Retrieve the aggregated count for a campaign metric from Redis.
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType event type string (CLICK, IMPRESSION, …)
     * @return {@link Optional} containing the count, or empty on cache miss / error
     */
    public Optional<Long> getCount(String tenantId, String campaignId, String metricType) {
        String key = buildKey(tenantId, campaignId);
        try {
            Object raw = redisTemplate.opsForHash().get(key, metricType);
            if (raw == null) {
                return Optional.empty();
            }
            return Optional.of(Long.parseLong(raw.toString()));
        } catch (Exception ex) {
            log.warn("Redis HGET failed for key={} field={}: {}", key, metricType, ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Check whether a campaign has any counter data in the hot tier.
     * Useful for health checks and debugging.
     */
    public boolean exists(String tenantId, String campaignId) {
        try {
            return Boolean.TRUE.equals(
                    redisTemplate.hasKey(buildKey(tenantId, campaignId)));
        } catch (Exception ex) {
            return false;
        }
    }

    private static String buildKey(String tenantId, String campaignId) {
        return "campaign:" + tenantId + ":" + campaignId;
    }
}

