package com.java.query.handler;

import com.java.query.dto.TimeSeriesPoint;

import java.time.Instant;
import java.util.List;

/**
 * Strategy contract for a single serving tier (architecture 5.2).
 *
 * <p>Each implementation encapsulates the read logic for exactly one tier:
 * <ul>
 *   <li>{@link RedisCacheTierHandler}   — hot window (&lt; 48 h)</li>
 *   <li>{@link PinotOlapTierHandler}    — warm window (&lt; 30 d)</li>
 *   <li>{@link TrinoLakehouseTierHandler} — cold window (&gt; 30 d)</li>
 * </ul>
 *
 * <p>Decouples {@link com.java.query.service.TieredInsightsEngine} from concrete store
 * classes (Dependency Inversion) and allows new tiers to be added without modifying
 * the engine (Open/Closed).
 */
public interface TierQueryHandler {

    /**
     * Resolve the aggregated scalar count for a campaign metric.
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType CLICK / IMPRESSION / CLICK_TO_BASKET
     * @param from       query window start (may be {@code null} — tier interprets as default)
     * @return aggregated count; {@code 0} if the store is unavailable
     */
    long resolveCount(String tenantId, String campaignId, String metricType, Instant from);

    /**
     * Resolve a grain-bucketed time series for a campaign metric.
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType CLICK / IMPRESSION / CLICK_TO_BASKET
     * @param from       effective window start (never {@code null} — caller normalises)
     * @param to         effective window end   (never {@code null} — caller normalises)
     * @param grain      {@code minute}, {@code hour}, or {@code day}
     * @return ordered list of bucketed data points; never {@code null}
     */
    List<TimeSeriesPoint> resolveTimeSeries(String tenantId, String campaignId, String metricType,
                                            Instant from, Instant to, String grain);
}

