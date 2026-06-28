package com.java.query.service;

import com.java.query.dto.CampaignMetricResponse;
import com.java.query.dto.TimeSeriesPoint;
import com.java.query.observability.TierQueryContext;
import com.java.query.router.TierRoutingEngine;
import com.java.security.paseto.PasetoClaims;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * Default {@link InsightsService} implementation.
 *
 * <p>Responsibilities after refactoring (B5/B6 SRP + A3 DRY + C2 KISS):
 * <ol>
 *   <li>Input validation + authorization → delegated to {@link InsightsRequestValidator}</li>
 *   <li>Time-window parsing and defaulting (uses {@link QueryService#DEFAULT_HOT_WINDOW})</li>
 *   <li>Time-series query via {@link QueryService}</li>
 *   <li>DTO assembly (tier label comes from the query-service response field)</li>
 * </ol>
 *
 * <p>{@link TierRoutingEngine} is no longer injected here — the tier is an
 * internal routing detail of {@link TieredInsightsEngine} and is surfaced to
 * callers only via the {@code source} field of {@link CampaignMetricResponse}.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InsightsServiceImpl implements InsightsService {

    private final QueryService              queryService;
    private final InsightsRequestValidator  validator;

    @Override
    public CampaignMetricResponse getMetrics(
            String tenantId, PasetoClaims claims,
            String campaignId, String metricType,
            String from, String to, String grain, String placement) {

        log.debug("[SERVICE] getMetrics start tenant={} campaign={} metric={} grain={} from={} to={}",
                tenantId, campaignId, metricType, grain, from, to);

        // ---- Validate + authorise (single responsibility) -------------------
        validator.validate(tenantId, campaignId, grain, placement, claims);

        // ---- Parse + default time window (C2 — uses shared constant) --------
        Instant fromInstant   = parseInstant(from);
        Instant toInstant     = parseInstant(to);
        Instant effectiveTo   = (toInstant   != null) ? toInstant   : Instant.now();
        Instant effectiveFrom = (fromInstant != null) ? fromInstant
                : effectiveTo.minus(QueryService.DEFAULT_HOT_WINDOW);

        // ---- Query -----------------------------------------------------------
        long queryStartMs = System.currentTimeMillis();
        List<TimeSeriesPoint> series = queryService.getTimeSeries(
                tenantId, campaignId, metricType,
                fromInstant, toInstant, grain, placement);
        long dataFreshnessMs = System.currentTimeMillis() - queryStartMs;

        long total = series.stream().mapToLong(TimeSeriesPoint::value).sum();

        log.info("[SERVICE] getMetrics completed tenant={} campaign={} metric={} "
                        + "points={} total={} queryMs={}",
                tenantId, campaignId, metricType, series.size(), total, dataFreshnessMs);

        return new CampaignMetricResponse(
                tenantId,
                campaignId,
                metricType.toLowerCase(),
                effectiveFrom.toString(),   // C2: always use parsed / defaulted instant
                effectiveTo.toString(),
                grain,
                (placement != null && !placement.isBlank()) ? placement : null,
                series,
                total,
                dataFreshnessMs,
                resolveSource());
    }

    /** Resolve tier label from the query-service context (read from TierQueryContext). */
    private static String resolveSource() {
        TierQueryContext.Context context =
                TierQueryContext.current();
        return context != null ? context.tierLabel() : "unknown";
    }

    /** Null-safe ISO-8601 parse; invalid/empty input yields {@code null}. */
    private static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException ex) {
            return null;
        }
    }
}
