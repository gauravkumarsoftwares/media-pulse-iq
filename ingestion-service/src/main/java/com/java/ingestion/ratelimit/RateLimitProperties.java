package com.java.ingestion.ratelimit;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Per-tenant rate-limit configuration (architecture §6.2 Noisy-Neighbour Protection).
 *
 * <p>Bound from {@code platform.rate-limit.*} in application[-profile].yml.
 *
 * <h2>Tiered quota model</h2>
 * <pre>
 *  tier         | default events/s
 *  -------------|-----------------
 *  enterprise   |  10 000
 *  standard     |   1 000
 *  free         |     100
 *  (default)    |     500  ← applies when tenant not in tenant-overrides
 * </pre>
 *
 * <p>Tenant-specific overrides take precedence over tier defaults.
 */
@Component
@ConfigurationProperties(prefix = "platform.rate-limit")
@Getter
@Setter
public class RateLimitProperties {

    /** Default max events per second when no tenant-specific override is configured. */
    private int defaultEventsPerSecond = 500;

    /** Window duration in seconds used for the sliding counter bucket. */
    private int windowSeconds = 1;

    /**
     * Per-tenant override map.
     * Key = tenantId (exact match), Value = max events allowed in the configured window.
     * Example YAML:
     * <pre>
     *   platform.rate-limit.tenant-overrides:
     *     walmart_us: 10000
     *     small_shop: 50
     * </pre>
     */
    private Map<String, Integer> tenantOverrides = new HashMap<>();

    /** When true, requests exceeding the limit get HTTP 429; false = log-only (warn mode). */
    private boolean enforced = true;

    /**
     * Redis configuration for the distributed rate-limit counter.
     * Falls back to {@code spring.data.redis.*}.
     */
    private int redisTtlMultiplier = 2;   // key TTL = windowSeconds × ttlMultiplier

    /**
     * Resolve the effective quota for a tenant.
     *
     * @param tenantId the tenant identifier
     * @return max events allowed within the configured window
     */
    public int quotaFor(String tenantId) {
        return tenantOverrides.getOrDefault(tenantId, defaultEventsPerSecond);
    }
}

