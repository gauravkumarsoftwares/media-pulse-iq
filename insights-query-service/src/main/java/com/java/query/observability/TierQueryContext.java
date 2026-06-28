package com.java.query.observability;

import com.java.query.router.QueryTier;

/**
 * ThreadLocal carrier that transfers the runtime-resolved query context
 * from {@link com.java.query.service.TieredInsightsEngine} to
 * {@link QueryMetricsAspect} without threading extra parameters through the call stack.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li><strong>Bind</strong> — called by the engine immediately after tier resolution,
 *       before handler dispatch: {@link #bind(QueryTier, String, String)}.</li>
 *   <li><strong>Read</strong> — called by {@link QueryMetricsAspect} inside its
 *       {@code finally} block to obtain tier/tenant/metric tags for Micrometer.</li>
 *   <li><strong>Clear</strong> — called by the aspect (never by the engine) to prevent
 *       stale context leaking across reused thread-pool threads:
 *       {@link #clear()}.</li>
 * </ol>
 *
 * <p><strong>Thread safety:</strong> each {@link ThreadLocal} slot is independent per
 * thread; concurrent requests on different threads never share state.
 */
public final class TierQueryContext {

    /**
     * Immutable snapshot of the per-request routing decision.
     *
     * @param tierLabel  lowercase tier name for Micrometer tag (e.g. {@code redis_cache})
     * @param tenantId   tenant scope, used as a Micrometer tag
     * @param metricType event type (CLICK / IMPRESSION / CLICK_TO_BASKET)
     */
    public record Context(String tierLabel, String tenantId, String metricType) {}

    private static final ThreadLocal<Context> HOLDER = new ThreadLocal<>();

    /**
     * Bind the current thread's query context.
     * Called by the engine <em>before</em> delegating to a {@link com.java.query.handler.TierQueryHandler}.
     */
    public static void bind(QueryTier tier, String tenantId, String metricType) {
        HOLDER.set(new Context(tier.name().toLowerCase(), tenantId, metricType));
    }

    /**
     * Read the current thread's context.
     *
     * @return the bound {@link Context}, or {@code null} if {@link #bind} was not yet called
     *         (e.g. if an exception was thrown before tier resolution)
     */
    public static Context current() {
        return HOLDER.get();
    }

    /**
     * Remove the context to prevent thread-pool leaks.
     * Always called by {@link QueryMetricsAspect} in its {@code finally} block — never by the engine.
     */
    public static void clear() {
        HOLDER.remove();
    }

    private TierQueryContext() {}
}

