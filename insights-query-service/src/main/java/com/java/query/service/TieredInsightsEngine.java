package com.java.query.service;

import com.java.query.dto.TimeSeriesPoint;
import com.java.query.handler.PinotOlapTierHandler;
import com.java.query.handler.RedisCacheTierHandler;
import com.java.query.handler.TierQueryHandler;
import com.java.query.handler.TrinoLakehouseTierHandler;
import com.java.query.observability.TierQueryContext;
import com.java.query.observability.TieredQuery;
import com.java.query.router.QueryTier;
import com.java.query.router.TierRoutingEngine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.java.query.service.QueryService.DEFAULT_HOT_WINDOW;

/**
 * Production {@link QueryService} routing to the three serving tiers (architecture 5.2).
 *
 * <h3>Tier routing</h3>
 * <pre>
 *   window &lt; 48h  =&gt; REDIS_CACHE     (P99 &lt; 7.5 ms)
 *   window &lt; 30d  =&gt; PINOT_OLAP      (P99 &lt; 85 ms)
 *   window &gt; 30d  =&gt; TRINO_LAKEHOUSE (P99 &lt; 4.2 s, Iceberg/S3)
 * </pre>
 *
 * <h3>Design</h3>
 * <p>This class is intentionally a <em>thin dispatcher</em>; all per-tier store logic lives in
 * dedicated {@link TierQueryHandler} implementations (Strategy pattern,
 * OCP — adding a new tier requires no changes here).
 *
 * <p>Metrics instrumentation (latency timer + query counter) is handled entirely by
 * {@link com.java.query.observability.QueryMetricsAspect} via {@link TieredQuery @TieredQuery},
 * using {@link TierQueryContext} as a ThreadLocal bridge to pass the runtime-resolved tier
 * to the aspect without polluting the method signatures.
 *
 * <p>
 * // TODO: Known approximations and deferred work —
 * //  [REDIS-TS]   REDIS_CACHE time-series uses even distribution; replace with ZRANGEBYSCORE scan
 * //  [PINOT-TS]   PINOT_OLAP time-series uses even distribution; replace with dateTimeConvert GROUP BY
 * //  [TRINO-FB]   StarTree fallback in TRINO_LAKEHOUSE tier should be removed once Trino is stable
 * //  [BUCKET-CAP] 10 000-bucket safety cap should be externalised to application.yml
 * //  [STARTREE]   StarTreeStore is an in-memory stub; wire real Pinot StarTree index in production
 */
@Service
@Primary
@Slf4j
public class TieredInsightsEngine implements QueryService {

    private final TierRoutingEngine              tierRoutingEngine;
    private final Map<QueryTier, TierQueryHandler> handlers;

    public TieredInsightsEngine(TierRoutingEngine       tierRoutingEngine,
                                 RedisCacheTierHandler   redisHandler,
                                 PinotOlapTierHandler    pinotHandler,
                                 TrinoLakehouseTierHandler trinoHandler) {
        this.tierRoutingEngine = tierRoutingEngine;
        this.handlers = Map.of(
                QueryTier.REDIS_CACHE,     redisHandler,
                QueryTier.PINOT_OLAP,      pinotHandler,
                QueryTier.TRINO_LAKEHOUSE, trinoHandler);
    }

    // ---- Scalar aggregate -----------------------------------------------

    @Override
    public long getCampaignCount(String tenantId, String campaignId, String metricType) {
        return getCampaignCount(tenantId, campaignId, metricType, null);
    }

    /**
     * Resolves the serving tier, binds it to {@link TierQueryContext} for the AOP aspect,
     * then delegates to the matching {@link TierQueryHandler}.
     *
     * <p>{@link TieredQuery @TieredQuery} signals {@link com.java.query.observability.QueryMetricsAspect}
     * to wrap this method with a Micrometer latency timer. The aspect reads the tier/tenant/metric
     * tags from {@link TierQueryContext} after the method body has executed.
     */
    @Override
    @TieredQuery
    public long getCampaignCount(String tenantId, String campaignId,
                                  String metricType, Instant from) {
        QueryTier tier = tierRoutingEngine.resolveTier(from);
        TierQueryContext.bind(tier, tenantId, metricType);
        log.debug("getCampaignCount tenant={} campaign={} metric={} tier={}",
                tenantId, campaignId, metricType, tier);
        return handlers.get(tier).resolveCount(tenantId, campaignId, metricType, from);
    }

    // ---- Time-series (grain-bucketed) ------------------------------------

    /**
     * Normalises the time window, resolves the tier, binds {@link TierQueryContext},
     * then delegates to the matching {@link TierQueryHandler}.
     *
     * <p>Returns typed {@link TimeSeriesPoint} records directly — no raw
     * {@code Map<String,Object>} casts needed at the caller.
     */
    @Override
    @TieredQuery
    public List<TimeSeriesPoint> getTimeSeries(String tenantId, String campaignId,
                                                String metricType,
                                                Instant from, Instant to,
                                                String grain, String placement) {
        Instant effectiveTo   = (to   != null) ? to   : Instant.now();
        Instant effectiveFrom = (from != null) ? from : effectiveTo.minus(DEFAULT_HOT_WINDOW);

        QueryTier tier = tierRoutingEngine.resolveTier(effectiveFrom);
        TierQueryContext.bind(tier, tenantId, metricType);
        log.debug("getTimeSeries tenant={} campaign={} metric={} grain={} tier={}",
                tenantId, campaignId, metricType, grain, tier);
        return handlers.get(tier).resolveTimeSeries(
                tenantId, campaignId, metricType, effectiveFrom, effectiveTo, grain);
    }
}
