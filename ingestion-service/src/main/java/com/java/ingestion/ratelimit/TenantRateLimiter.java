package com.java.ingestion.ratelimit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Distributed per-tenant rate limiter using Redis atomic counters
 * (architecture 6.2 — Noisy-Neighbour Protection).
 *
 * <h2>Algorithm: Fixed-Window Counter (Redis INCR + EXPIRE)</h2>
 * <pre>
 *  key  = ratelimit:{tenantId}:{epoch_bucket}
 *         where epoch_bucket = floor(epochSeconds / windowSeconds)
 *  flow = INCR key  → count
 *         if count == 1 → EXPIRE key (windowSeconds × ttlMultiplier)
 *         if count > quota → 429 Too Many Requests
 * </pre>
 *
 * <p>The INCR is atomic in Redis (single round-trip, no Lua needed for the
 * fixed-window case). For production, upgrade to a Lua-based <em>sliding</em>
 * window or use Redis Cell (rate-limiting module) to avoid boundary bursts.
 *
 * <p>If Redis is unavailable, the limiter fails <em>open</em> (logs a warning
 * and allows the request) — a deliberate choice to favour availability over
 * strict quota enforcement during infrastructure failures.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TenantRateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final RateLimitProperties props;

    /**
     * Check whether the tenant is within quota for the current time window.
     *
     * @param tenantId the tenant performing the request
     * @return {@code true} if the request is allowed; {@code false} if it should be rejected
     */
    public boolean isAllowed(String tenantId) {
        if (!props.isEnforced()) {
            return true;   // warn-only mode — always allow
        }
        try {
            return checkRedis(tenantId);
        } catch (Exception ex) {
            // Fail-open: Redis unavailable → allow the request and alert.
            log.warn("Rate-limiter Redis call failed for tenant '{}' — failing open: {}",
                    tenantId, ex.getMessage());
            return true;
        }
    }

    /**
     * Return the remaining quota for the tenant in the current window.
     * Returns -1 if Redis is unavailable.
     */
    public long remainingQuota(String tenantId) {
        try {
            long bucket = currentBucket();
            String key = buildKey(tenantId, bucket);
            String raw = redisTemplate.opsForValue().get(key);
            long used = raw == null ? 0L : Long.parseLong(raw);
            return Math.max(0, props.quotaFor(tenantId) - used);
        } catch (Exception ex) {
            return -1L;
        }
    }

    // ---- private helpers -------------------------------------------------

    private boolean checkRedis(String tenantId) {
        long bucket = currentBucket();
        String key = buildKey(tenantId, bucket);
        int quota = props.quotaFor(tenantId);

        Long count = redisTemplate.opsForValue().increment(key);
        if (count == null) {
            return true;  // unexpected null → fail open
        }
        if (count == 1L) {
            // First request in this window: set TTL to prevent orphaned keys.
            long ttlSeconds = (long) props.getWindowSeconds() * props.getRedisTtlMultiplier();
            redisTemplate.expire(key, Duration.ofSeconds(ttlSeconds));
        }
        boolean allowed = count <= quota;
        if (!allowed) {
            log.warn("Rate limit exceeded: tenant='{}' count={} quota={} bucket={}",
                    tenantId, count, quota, bucket);
        }
        return allowed;
    }

    /** Epoch-second bucket index: floor(now / windowSeconds). */
    private long currentBucket() {
        return System.currentTimeMillis() / 1000L / props.getWindowSeconds();
    }

    /** Redis key pattern: {@code ratelimit:{tenantId}:{bucket}}. */
    private static String buildKey(String tenantId, long bucket) {
        return "ratelimit:" + tenantId + ":" + bucket;
    }
}

