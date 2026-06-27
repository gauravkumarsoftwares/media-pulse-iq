package com.java.query.store;

import com.java.query.config.TrinoProperties;
import com.java.query.reconciliation.CampaignKey;
import com.java.query.reconciliation.CampaignMetricCount;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Production cold-tier query client for Trino-on-Iceberg (architecture 4.3).
 *
 * <h2>Why Trino + Iceberg for cold reads?</h2>
 * <p>Events older than 30 days are stored as Parquet files in an Apache Iceberg table
 * on S3 (written by Flink).  Trino federates distributed SQL scans over those files
 * without moving data: ~90% cheaper than keeping all historical data in Pinot.
 *
 * <h2>Performance &amp; scalability design</h2>
 * <ul>
 *   <li><strong>HikariCP connection pool</strong> — connections are reused across
 *       queries; no per-query TCP/TLS handshake overhead.  Pool is intentionally
 *       small (default: 10) since cold queries are rare and each already uses
 *       the Trino cluster's internal parallelism.</li>
 *   <li><strong>Iceberg partition pruning</strong> — every query includes an
 *       {@code event_date} predicate matching Iceberg's daily partition scheme,
 *       eliminating the vast majority of S3 file scans at the metadata level.</li>
 *   <li><strong>Parameterized PreparedStatements</strong> — user-supplied values
 *       (tenant, campaign, metric type) are always bound via {@code setString()},
 *       never string-concatenated, preventing SQL injection (OWASP A03).</li>
 *   <li><strong>Batch grouped query</strong> — {@link #queryGroupedCounts} retrieves
 *       all campaign counts for a window in a single Trino query, so the daily
 *       reconciliation job pays <em>one</em> scan instead of N+1 per campaign.</li>
 *   <li><strong>Native time-series bucketing</strong> — {@link #queryTimeSeries} uses
 *       Trino's {@code date_trunc + GROUP BY} for true bucketed results instead of
 *       an approximation.</li>
 *   <li><strong>Per-statement query timeout</strong> — {@code Statement.setQueryTimeout}
 *       prevents runaway scans from stalling the reconciliation job.</li>
 *   <li><strong>Micrometer timers</strong> — {@code ads.trino.query.latency} tagged
 *       by query type for Grafana P99 alerting.</li>
 * </ul>
 *
 * <h2>Graceful degradation</h2>
 * <p>When {@code platform.trino.enabled=false} (local/dev) or the {@code DataSource}
 * bean is absent, all methods return {@code 0} / empty list immediately without
 * attempting a connection.
 *
 * <h2>SQL patterns</h2>
 * <pre>
 *   -- queryCount
 *   SELECT COUNT(*) FROM iceberg.ads.shopping_events
 *   WHERE tenant_id = ?  AND campaign_id = ?  AND event_type = ?
 *     AND event_date &gt;= DATE ?  AND event_date &lt;= DATE ?
 *
 *   -- queryGroupedCounts (batch for reconciliation)
 *   SELECT tenant_id, campaign_id, event_type, COUNT(*) AS cnt
 *   FROM iceberg.ads.shopping_events
 *   WHERE event_date &gt;= DATE ?  AND event_date &lt;= DATE ?
 *   GROUP BY tenant_id, campaign_id, event_type  LIMIT ?
 *
 *   -- queryTimeSeries (native bucketed series)
 *   SELECT date_trunc('hour', from_unixtime(event_timestamp_ms / 1000)) AS bucket,
 *          COUNT(*) AS cnt
 *   FROM iceberg.ads.shopping_events
 *   WHERE tenant_id = ?  AND campaign_id = ?  AND event_type = ?
 *     AND event_date &gt;= DATE ?  AND event_date &lt;= DATE ?
 *   GROUP BY 1  ORDER BY 1
 * </pre>
 */
@Component
@Slf4j
public class TrinoIcebergClient {

    // ---- SQL templates (catalog/schema/table are fixed config, not user input) ----

    /** Scalar COUNT with partition-pruning date bounds. */
    private final String sqlQueryCount;

    /** Batch GROUP BY — one query replaces N+1 per-campaign lookups in reconciliation. */
    private final String sqlQueryGrouped;

    /**
     * Time-series template: {@code %s} = date_trunc grain (resolved from allow-list,
     * never raw user input — substituted with {@link String#format} before execution).
     */
    private final String sqlTimeSeriesTemplate;

    private final TrinoProperties props;
    private final MeterRegistry meterRegistry;

    /**
     * Injected from the {@code trinoDataSource} HikariCP bean (created by
     * {@link com.java.query.config.PlatformInfraConfig} when
     * {@code platform.trino.enabled=true}).
     * {@code null} when Trino is disabled — all methods short-circuit safely.
     */
    @Nullable
    private final DataSource dataSource;

    /**
     * Spring constructor injection.  The {@code dataSource} parameter is optional:
     * when the {@code trinoDataSource} bean is absent, Spring injects {@code null}
     * and the client returns stub values for all queries.
     */
    @Autowired
    public TrinoIcebergClient(TrinoProperties props,
                               MeterRegistry meterRegistry,
                               @Nullable @Qualifier("trinoDataSource") DataSource dataSource) {
        this.props = props;
        this.meterRegistry = meterRegistry;
        this.dataSource = dataSource;

        // Fully-qualified table name: catalog.schema.table (fixed config, safe to format).
        String fqt = props.getCatalog() + "." + props.getSchema() + "." + props.getTable();

        this.sqlQueryCount = "SELECT COUNT(*) AS total FROM " + fqt
                + " WHERE tenant_id = ? AND campaign_id = ? AND event_type = ?"
                + " AND event_date >= DATE ? AND event_date <= DATE ?";

        this.sqlQueryGrouped = "SELECT tenant_id, campaign_id, event_type, COUNT(*) AS cnt"
                + " FROM " + fqt
                + " WHERE event_date >= DATE ? AND event_date <= DATE ?"
                + " GROUP BY tenant_id, campaign_id, event_type LIMIT ?";

        // %s is replaced by grain (minute|hour|day) from sanitizeGrain() — not user input.
        this.sqlTimeSeriesTemplate = "SELECT date_trunc('%s', from_unixtime(event_timestamp_ms / 1000)) AS bucket,"
                + " COUNT(*) AS cnt"
                + " FROM " + fqt
                + " WHERE tenant_id = ? AND campaign_id = ? AND event_type = ?"
                + " AND event_date >= DATE ? AND event_date <= DATE ?"
                + " GROUP BY 1 ORDER BY 1";
    }

    // =========================================================================
    //  Public query methods
    // =========================================================================

    /**
     * Scalar COUNT for a single campaign metric over the specified time window.
     *
     * <p>The {@code from}/{@code to} instants are mapped to UTC calendar dates and
     * used as Iceberg partition predicates — only relevant daily partitions are opened.
     *
     * @param tenantId   tenant scope (from verified PASETO claims)
     * @param campaignId campaign identifier (allow-list validated by controller)
     * @param metricType CLICK / IMPRESSION / CLICK_TO_BASKET
     * @param from       window start (UTC date for partition pruning)
     * @param to         window end   (UTC date for partition pruning)
     * @return aggregated count, or {@code 0} when Trino is unavailable / query fails
     */
    public long queryCount(String tenantId, String campaignId, String metricType,
                           Instant from, Instant to) {
        if (!isAvailable()) return 0L;

        Timer.Sample sample = Timer.start(meterRegistry);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlQueryCount)) {

            stmt.setQueryTimeout(props.getQueryTimeoutSeconds());
            stmt.setString(1, tenantId);
            stmt.setString(2, campaignId);
            stmt.setString(3, metricType);
            stmt.setString(4, toDateString(from));
            stmt.setString(5, toDateString(to));

            try (ResultSet rs = stmt.executeQuery()) {
                long result = rs.next() ? rs.getLong("total") : 0L;
                log.debug("[trino] queryCount tenant={} campaign={} metric={} [{},{}] → {}",
                        tenantId, campaignId, metricType, toDateString(from), toDateString(to), result);
                return result;
            }
        } catch (SQLException ex) {
            log.warn("[trino] queryCount failed tenant={} campaign={} metric={}: {}",
                    tenantId, campaignId, metricType, ex.getMessage());
            return 0L;
        } finally {
            recordTimer(sample, "count");
        }
    }

    /**
     * Batch grouped COUNT for the daily reconciliation job (Pinot vs Iceberg comparison).
     *
     * <p>Returns all {@code (tenant_id, campaign_id, event_type)} groups with their
     * counts in the given epoch-ms window — <strong>one Trino query</strong> per
     * reconciliation run instead of one per campaign.  At 10 000 campaigns this
     * reduces latency from minutes to seconds.
     *
     * @param fromMs epoch ms (inclusive lower bound)
     * @param toMs   epoch ms (exclusive upper bound)
     * @param limit  maximum groups to return (matches {@code max-campaigns-per-run})
     * @return list of campaign metric counts; empty if Trino is unavailable or query fails
     */
    public List<CampaignMetricCount> queryGroupedCounts(long fromMs, long toMs, int limit) {
        if (!isAvailable()) {
            log.debug("[trino][reconciliation] Trino not available — returning empty grouped counts");
            return Collections.emptyList();
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlQueryGrouped)) {

            stmt.setQueryTimeout(props.getQueryTimeoutSeconds());
            stmt.setString(1, toDateStringFromMs(fromMs));
            stmt.setString(2, toDateStringFromMs(toMs));
            stmt.setInt(3, limit);

            List<CampaignMetricCount> results = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new CampaignMetricCount(
                            new CampaignKey(
                                    rs.getString("tenant_id"),
                                    rs.getString("campaign_id"),
                                    rs.getString("event_type")),
                            rs.getLong("cnt")));
                }
            }
            log.debug("[trino][reconciliation] queryGroupedCounts [{},{}] → {} groups",
                    toDateStringFromMs(fromMs), toDateStringFromMs(toMs), results.size());
            return results;

        } catch (SQLException ex) {
            log.warn("[trino][reconciliation] queryGroupedCounts failed: {}", ex.getMessage());
            return Collections.emptyList();
        } finally {
            recordTimer(sample, "grouped");
        }
    }

    /**
     * Native time-series query using Trino {@code date_trunc + GROUP BY}.
     *
     * <p>Returns an ordered list of {@code {timestamp, value}} maps, one per grain
     * bucket.  This is the cold-tier equivalent of the Pinot {@code dateTimeConvert}
     * GROUP BY query and produces <em>real</em> bucketed data rather than an
     * evenly-distributed approximation.
     *
     * <p>The {@code grain} string is validated against a strict allow-list in
     * {@link #sanitizeGrain} — the raw value is <em>never</em> embedded in SQL.
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType CLICK / IMPRESSION / CLICK_TO_BASKET
     * @param from       window start
     * @param to         window end
     * @param grain      {@code minute}, {@code hour}, or {@code day}
     * @return ordered list of {@code {timestamp, value}} buckets; empty on error
     */
    public List<Map<String, Object>> queryTimeSeries(String tenantId, String campaignId,
                                                      String metricType,
                                                      Instant from, Instant to,
                                                      String grain) {
        if (!isAvailable()) return Collections.emptyList();

        // sanitizeGrain returns one of {"minute","hour","day"} — safe to format into SQL.
        String sql = String.format(sqlTimeSeriesTemplate, sanitizeGrain(grain));

        Timer.Sample sample = Timer.start(meterRegistry);
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setQueryTimeout(props.getQueryTimeoutSeconds());
            stmt.setString(1, tenantId);
            stmt.setString(2, campaignId);
            stmt.setString(3, metricType);
            stmt.setString(4, toDateString(from));
            stmt.setString(5, toDateString(to));

            List<Map<String, Object>> series = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Timestamp bucket = rs.getTimestamp("bucket");
                    Map<String, Object> point = new LinkedHashMap<>();
                    point.put("timestamp", bucket.toInstant().toString());
                    point.put("value", rs.getLong("cnt"));
                    series.add(point);
                }
            }
            log.debug("[trino] queryTimeSeries tenant={} campaign={} metric={} grain={} → {} buckets",
                    tenantId, campaignId, metricType, grain, series.size());
            return series;

        } catch (SQLException ex) {
            log.warn("[trino] queryTimeSeries failed tenant={} campaign={} metric={}: {}",
                    tenantId, campaignId, metricType, ex.getMessage());
            return Collections.emptyList();
        } finally {
            recordTimer(sample, "timeseries");
        }
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    private boolean isAvailable() {
        if (!props.isEnabled() || dataSource == null) {
            log.trace("[trino] not available (enabled={}, dataSource={})",
                    props.isEnabled(), dataSource != null ? "present" : "null");
            return false;
        }
        return true;
    }

    private void recordTimer(Timer.Sample sample, String queryType) {
        sample.stop(Timer.builder("ads.trino.query.latency")
                .description("Trino cold-tier query latency by type")
                .tag("queryType", queryType)
                .register(meterRegistry));
    }

    /** Convert an {@link Instant} to a UTC calendar-date string ({@code yyyy-MM-dd}). */
    private static String toDateString(Instant instant) {
        return instant.atZone(ZoneOffset.UTC).toLocalDate().toString();
    }

    private static String toDateStringFromMs(long epochMs) {
        return toDateString(Instant.ofEpochMilli(epochMs));
    }

    /**
     * Validates the caller-supplied grain against a strict allow-list.
     * The returned value is one of {@code "minute"}, {@code "hour"}, {@code "day"} —
     * it is safe to substitute into a SQL string via {@link String#format}.
     */
    private static String sanitizeGrain(String grain) {
        if (grain == null) return "hour";
        return switch (grain.toLowerCase()) {
            case "minute" -> "minute";
            case "day"    -> "day";
            default       -> "hour";
        };
    }
}
