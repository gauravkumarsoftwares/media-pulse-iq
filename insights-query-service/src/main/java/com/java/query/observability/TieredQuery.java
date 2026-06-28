package com.java.query.observability;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link com.java.query.service.QueryService} method as a tiered query entry point.
 *
 * <p>{@link QueryMetricsAspect} intercepts every method carrying this annotation via
 * {@code @Around}, wrapping it with a Micrometer latency timer and a query counter.
 * The tier/tenant/metricType tags are read from {@link TierQueryContext} which the
 * engine binds to the current thread immediately after resolving the serving tier.
 *
 * <h3>Usage</h3>
 * <pre>
 *   {@literal @}Override
 *   {@literal @}TieredQuery
 *   public long getCampaignCount(String tenantId, String campaignId,
 *                                String metricType, Instant from) { ... }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TieredQuery {
}

