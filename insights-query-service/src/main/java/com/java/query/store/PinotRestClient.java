package com.java.query.store;

import com.java.query.config.PinotProperties;
import com.java.query.dto.TimeSeriesPoint;
import com.java.query.reconciliation.CampaignKey;
import com.java.query.reconciliation.CampaignMetricCount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Apache Pinot query client for the warm serving tier (architecture 4).
 *
 * <h3>SQL patterns</h3>
 * <pre>
 *   -- queryCount (scalar)
 *   SELECT count(*) FROM shopping_events
 *   WHERE tenant_id = '?' AND campaign_id = '?' AND event_type = '?'
 *
 *   -- queryTimeSeries (PINOT-TS fix — real dateTimeConvert GROUP BY)
 *   SELECT dateTimeConvert(event_timestamp_ms,'1:MILLISECONDS:EPOCH','1:HOURS:EPOCH','1:HOURS') AS bucket,
 *          count(*) AS cnt
 *   FROM shopping_events
 *   WHERE tenant_id = '?' AND campaign_id = '?' AND event_type = '?'
 *     AND event_timestamp_ms >= ? AND event_timestamp_ms <= ?
 *   GROUP BY bucket ORDER BY bucket
 *
 *   -- queryGroupedCounts (reconciliation)
 *   SELECT tenant_id, campaign_id, event_type, count(*)
 *   FROM shopping_events
 *   WHERE event_timestamp_ms >= ? AND event_timestamp_ms < ?
 *   GROUP BY tenant_id, campaign_id, event_type
 *   LIMIT ?
 * </pre>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PinotRestClient {

    private final Connection       pinotConnection;
    private final PinotProperties  pinotProperties;

    // ---- Scalar count -------------------------------------------------------

    public long queryCount(String tenantId, String campaignId, String metricType) {
        if (!pinotProperties.isEnabled()) return 0L;

        String sql = String.format(
                "SELECT count(*) FROM %s WHERE tenant_id = '%s' AND campaign_id = '%s' AND event_type = '%s'",
                pinotProperties.getTable(),
                escape(tenantId), escape(campaignId), escape(metricType));

        try {
            ResultSetGroup rsg = pinotConnection.execute(sql);
            if (rsg.getResultSetCount() == 0) return 0L;
            ResultSet rs = rsg.getResultSet(0);
            if (rs.getRowCount() == 0) return 0L;
            return rs.getLong(0, 0);
        } catch (PinotClientException ex) {
            log.warn("Pinot queryCount failed tenant={} campaign={} metric={}: {}",
                    tenantId, campaignId, metricType, ex.getMessage());
            return 0L;
        }
    }

    // ---- Time-series (PINOT-TS fix) -----------------------------------------

    /**
     * Query Pinot for exact per-bucket counts using {@code dateTimeConvert + GROUP BY}.
     * Replaces the even-distribution approximation in {@link com.java.query.handler.PinotOlapTierHandler}.
     *
     * @param from       window start (inclusive, used as epoch-ms lower bound)
     * @param to         window end   (inclusive, used as epoch-ms upper bound)
     * @param grain      {@code minute}, {@code hour} (default), or {@code day}
     * @return ordered list of time-series points; empty if Pinot disabled / query fails
     */
    public List<TimeSeriesPoint> queryTimeSeries(String tenantId, String campaignId,
                                                  String metricType,
                                                  Instant from, Instant to,
                                                  String grain) {
        if (!pinotProperties.isEnabled()) return Collections.emptyList();

        GrainSpec spec   = grainSpec(grain);
        String   sql     = String.format(
                "SELECT dateTimeConvert(event_timestamp_ms,'1:MILLISECONDS:EPOCH','%s','%s') AS bucket,"
                + " count(*) AS cnt"
                + " FROM %s"
                + " WHERE tenant_id = '%s' AND campaign_id = '%s' AND event_type = '%s'"
                + " AND event_timestamp_ms >= %d AND event_timestamp_ms <= %d"
                + " GROUP BY bucket ORDER BY bucket",
                spec.outputFormat(), spec.bucketSize(),
                pinotProperties.getTable(),
                escape(tenantId), escape(campaignId), escape(metricType),
                from.toEpochMilli(), to.toEpochMilli());

        try {
            ResultSetGroup rsg = pinotConnection.execute(sql);
            if (rsg.getResultSetCount() == 0) return Collections.emptyList();
            ResultSet rs = rsg.getResultSet(0);

            List<TimeSeriesPoint> series = new ArrayList<>(rs.getRowCount());
            for (int i = 0; i < rs.getRowCount(); i++) {
                long bucketValue = rs.getLong(i, 0);
                long count       = rs.getLong(i, 1);
                series.add(new TimeSeriesPoint(
                        bucketToInstant(bucketValue, grain).toString(), count));
            }
            log.debug("[pinot] queryTimeSeries tenant={} campaign={} metric={} grain={} → {} buckets",
                    tenantId, campaignId, metricType, grain, series.size());
            return series;
        } catch (PinotClientException ex) {
            log.warn("[pinot] queryTimeSeries failed tenant={} campaign={} metric={}: {}",
                    tenantId, campaignId, metricType, ex.getMessage());
            return Collections.emptyList();
        }
    }

    // ---- Reconciliation grouped count ---------------------------------------

    public List<CampaignMetricCount> queryGroupedCounts(long fromMs, long toMs, int limit) {
        if (!pinotProperties.isEnabled()) {
            log.debug("[reconciliation] Pinot disabled — returning empty grouped counts");
            return Collections.emptyList();
        }

        String sql = String.format(
                "SELECT tenant_id, campaign_id, event_type, count(*) "
                + "FROM %s "
                + "WHERE event_timestamp_ms >= %d AND event_timestamp_ms < %d "
                + "GROUP BY tenant_id, campaign_id, event_type "
                + "LIMIT %d",
                pinotProperties.getTable(), fromMs, toMs, limit);

        try {
            ResultSetGroup rsg = pinotConnection.execute(sql);
            return extractGroupedCounts(rsg);
        } catch (PinotClientException ex) {
            log.warn("[reconciliation] Pinot grouped-count query failed: {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    // ---- Helpers ------------------------------------------------------------

    private static List<CampaignMetricCount> extractGroupedCounts(ResultSetGroup rsg) {
        if (rsg == null || rsg.getResultSetCount() == 0) return Collections.emptyList();
        ResultSet rs = rsg.getResultSet(0);
        List<CampaignMetricCount> results = new ArrayList<>(rs.getRowCount());
        for (int i = 0; i < rs.getRowCount(); i++) {
            try {
                results.add(new CampaignMetricCount(
                        new CampaignKey(rs.getString(i, 0), rs.getString(i, 1), rs.getString(i, 2)),
                        rs.getLong(i, 3)));
            } catch (Exception ex) {
                log.debug("[reconciliation] Skipping malformed Pinot row at index {}: {}",
                        i, ex.getMessage());
            }
        }
        return results;
    }

    /** Grain spec for Pinot dateTimeConvert: output format and bucket size strings. */
    private record GrainSpec(String outputFormat, String bucketSize) {}

    private static GrainSpec grainSpec(String grain) {
        return switch (grain == null ? "hour" : grain.toLowerCase()) {
            case "minute" -> new GrainSpec("1:MINUTES:EPOCH", "1:MINUTES");
            case "day"    -> new GrainSpec("1:DAYS:EPOCH",    "1:DAYS");
            default       -> new GrainSpec("1:HOURS:EPOCH",   "1:HOURS");
        };
    }

    /**
     * Convert a Pinot dateTimeConvert bucket value back to an {@link Instant}.
     * Pinot returns hours/minutes/days since Unix epoch for the respective grains.
     */
    private static Instant bucketToInstant(long bucketValue, String grain) {
        long epochSec = switch (grain == null ? "hour" : grain.toLowerCase()) {
            case "minute" -> bucketValue * 60L;
            case "day"    -> bucketValue * 86_400L;
            default       -> bucketValue * 3_600L;
        };
        return Instant.ofEpochSecond(epochSec);
    }

    /** Single-quote escaping (SQL standard): replaces {@code '} with {@code ''}. */
    private static String escape(String value) {
        return value == null ? "" : value.replace("'", "''");
    }
}
