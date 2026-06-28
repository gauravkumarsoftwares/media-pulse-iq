package com.java.query.observability;

import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * AOP advice that replaces the manual {@code Timer.Sample try/finally} blocks that
 * previously littered every method in {@code TieredInsightsEngine}.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>The aspect starts a Micrometer timer sample <em>before</em> the method executes.</li>
 *   <li>The method body runs via {@link ProceedingJoinPoint#proceed()}.
 *       Inside that body, the engine calls {@link TierQueryContext#bind} to register
 *       the runtime-resolved tier/tenant/metricType on the current thread.</li>
 *   <li>In the {@code finally} block the aspect reads {@link TierQueryContext#current()}
 *       (null-safe fallback to {@code "unknown"} if tier resolution failed before bind),
 *       records the latency + counter via {@link QueryMetrics#stopQuery}, and then
 *       calls {@link TierQueryContext#clear()} to prevent thread-pool leaks.</li>
 * </ol>
 *
 * <h3>Why ThreadLocal (not annotation attributes)?</h3>
 * <p>The serving tier is determined <em>at runtime</em> inside the method body
 * ({@code tierRoutingEngine.resolveTier(from)}) and is therefore not known at
 * annotation-declaration time. Passing it via a {@link ThreadLocal} avoids reflection
 * on method arguments or a secondary service call from within the aspect.
 *
 * <h3>Self-invocation note</h3>
 * <p>Spring AOP uses CGLIB proxies; a direct {@code this.method()} call within the
 * same bean bypasses the proxy. Both the 3-arg and 4-arg overloads of
 * {@code getCampaignCount} are annotated so that either entry point is instrumented
 * regardless of which one is called from outside the proxy.
 */
@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class QueryMetricsAspect {

    private final QueryMetrics queryMetrics;

    /**
     * Wraps every method annotated with {@link TieredQuery} with a start/stop latency sample.
     *
     * <p>Tags (tier, tenantId, metricType) are read from {@link TierQueryContext}
     * which is populated by the engine <em>inside</em> the {@code proceed()} call.
     */
    @Around("@annotation(com.java.query.observability.TieredQuery)")
    public Object measureTieredQuery(ProceedingJoinPoint pjp) throws Throwable {
        Timer.Sample sample = queryMetrics.startQuery();
        try {
            return pjp.proceed();
        } finally {
            TierQueryContext.Context context = TierQueryContext.current();
            // Null-safe: ctx is null only if the method threw before reaching TierQueryContext.bind()
            String tier       = context != null ? context.tierLabel()  : "unknown";
            String tenantId   = context != null ? context.tenantId()   : "unknown";
            String metricType = context != null ? context.metricType() : "unknown";

            queryMetrics.stopQuery(sample, tier, tenantId, metricType);

            // Always clear — must not be left to the engine to avoid thread-pool leaks
            TierQueryContext.clear();

            log.trace("[metrics] tiered-query recorded tier={} tenant={} metric={}", tier, tenantId, metricType);
        }
    }
}

