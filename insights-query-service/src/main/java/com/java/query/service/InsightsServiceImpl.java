package com.java.query.service;

import com.java.query.common.ApiException;
import com.java.query.dto.CampaignMetricResponse;
import com.java.query.dto.TimeSeriesPoint;
import com.java.query.router.QueryTier;
import com.java.query.router.TierRoutingEngine;
import com.java.security.paseto.PasetoClaims;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default {@link InsightsService} implementation.
 *
 * <p>Responsibilities (Single Responsibility sub-concerns):
 * <ol>
 *   <li>Input allow-list validation (campaignId, placement, grain)</li>
 *   <li>Per-campaign authorization via {@code allowed_campaigns} claim</li>
 *   <li>Tier routing via {@link TierRoutingEngine}</li>
 *   <li>Time-series query via {@link QueryService}</li>
 *   <li>DTO mapping — raw {@code Map<String,Object>} → typed records</li>
 * </ol>
 */
@Service
@RequiredArgsConstructor
public class InsightsServiceImpl implements InsightsService {

    // Input allow-list (OWASP A03/A04): safe charset + max length.
    private static final Pattern ID_PATTERN   = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
    private static final Set<String> VALID_GRAINS = Set.of("minute", "hour", "day");

    private final QueryService      queryService;
    private final TierRoutingEngine tierRoutingEngine;

    @Override
    public CampaignMetricResponse getMetrics(
            String tenantId, PasetoClaims claims,
            String campaignId, String metricType,
            String from, String to, String grain, String placement) {

        // ---- Input validation -----------------------------------------------
        if (!ID_PATTERN.matcher(campaignId).matches()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Invalid campaignId format.");
        }
        if (placement != null && !placement.isBlank() && !ID_PATTERN.matcher(placement).matches()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Invalid placement format.");
        }
        if (!VALID_GRAINS.contains(grain)) {
            throw new ApiException(HttpStatus.BAD_REQUEST,
                    "Invalid grain. Allowed: minute, hour, day.");
        }

        // ---- Per-campaign authorization (OWASP A01) -------------------------
        if (claims != null && !claims.canAccessCampaign(campaignId)) {
            throw new ApiException(HttpStatus.FORBIDDEN,
                    "Not authorized for campaign: " + campaignId);
        }

        // ---- Query ----------------------------------------------------------
        Instant fromInstant = parseInstant(from);
        Instant toInstant   = parseInstant(to);
        String  toTs        = toInstant != null ? toInstant.toString() : Instant.now().toString();

        QueryTier tier = tierRoutingEngine.resolveTier(fromInstant);

        long queryStartMs = System.currentTimeMillis();
        List<Map<String, Object>> rawSeries = queryService.getTimeSeries(
                tenantId, campaignId, metricType, fromInstant, toInstant, grain, placement);
        long dataFreshnessMs = System.currentTimeMillis() - queryStartMs;

        // ---- Mapping: raw Map → typed DTO records ---------------------------
        List<TimeSeriesPoint> series = rawSeries.stream()
                .map(p -> new TimeSeriesPoint(
                        String.valueOf(p.get("timestamp")),
                        ((Number) p.get("value")).longValue()))
                .toList();

        long total = series.stream().mapToLong(TimeSeriesPoint::value).sum();

        return new CampaignMetricResponse(
                tenantId,
                campaignId,
                metricType.toLowerCase(),
                from != null ? from : Instant.now().minusSeconds(7200).toString(),
                toTs,
                grain,
                (placement != null && !placement.isBlank()) ? placement : null,
                series,
                total,
                dataFreshnessMs,
                tier.sourceLabel());
    }

    // ---- helpers ------------------------------------------------------------

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

