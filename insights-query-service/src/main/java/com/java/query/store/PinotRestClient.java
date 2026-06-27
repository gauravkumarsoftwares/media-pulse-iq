package com.java.query.store;

import com.java.query.config.PinotProperties;
import com.java.query.reconciliation.CampaignKey;
import com.java.query.reconciliation.CampaignMetricCount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.client.ResultSetGroup;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Apache Pinot query client for the warm serving tier (architecture 4).
 *
 * <p>Uses the <strong>native Apache Pinot Java SDK</strong>
 * ({@code pinot-java-client} — {@link Connection} /
 * {@link org.apache.pinot.client.transport.JsonAsyncHttpPinotClientTransportFactory})
 * rather than a raw HTTP client. Advantages over a plain
 * {@link org.springframework.web.client.RestClient}:
 * <ul>
 *   <li><strong>Async I/O + connection pooling</strong> — the SDK uses Netty
 *       via {@code async-http-client} internally, avoiding blocking JDK sockets.</li>
 *   <li><strong>Broker failover</strong> — the SDK rotates across broker replicas
 *       automatically on failure.</li>
 *   <li><strong>Typed result accessors</strong> — {@link ResultSet#getLong},
 *       {@link ResultSet#getString}, etc., replace manual JSON unmarshalling.</li>
 *   <li><strong>Pinot-native exceptions</strong> — {@link PinotClientException}
 *       surfaces Pinot error codes for targeted error handling.</li>
 *   <li><strong>Auth handled at transport layer</strong> — Basic / Bearer headers are
 *       set once when the {@link Connection} bean is created in
 *       {@link com.java.query.config.PlatformInfraConfig} and applied transparently
 *       to every request, driven by {@link PinotProperties#getAuthScheme()}.</li>
 * </ul>
 *
 * <h3>SQL patterns</h3>
 * <pre>
 *   -- queryCount (scalar)
 *   SELECT count(*) FROM shopping_events
 *   WHERE tenant_id = '?' AND campaign_id = '?' AND event_type = '?'
 *
 *   -- queryGroupedCounts (reconciliation)
 *   SELECT tenant_id, campaign_id, event_type, count(*)
 *   FROM shopping_events
 *   WHERE event_timestamp_ms &gt;= ? AND event_timestamp_ms &lt; ?
 *   GROUP BY tenant_id, campaign_id, event_type
 *   LIMIT ?
 * </pre>
 *
 * <p>Returns {@code 0} / empty list on any error, allowing the tier router to
 * degrade gracefully.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PinotRestClient {

    private final Connection pinotConnection;
    private final PinotProperties pinotProperties;

    /**
     * Query the aggregated count of {@code metricType} events for the given
     * tenant / campaign combination.
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign identifier
     * @param metricType event type string (CLICK, IMPRESSION, CLICK_TO_BASKET)
     * @return aggregated count, or {@code 0} on any error / empty result
     */
    public long queryCount(String tenantId, String campaignId, String metricType) {
        if (!pinotProperties.isEnabled()) {
            return 0L;
        }

        // Build parameterised SQL — identifiers already validated by the controller (OWASP A03).
        String sql = String.format(
                "SELECT count(*) FROM %s WHERE tenant_id = '%s' AND campaign_id = '%s' AND event_type = '%s'",
                pinotProperties.getTable(),
                escape(tenantId),
                escape(campaignId),
                escape(metricType));

        try {
            ResultSetGroup resultSetGroup = pinotConnection.execute(sql);
            if (resultSetGroup.getResultSetCount() == 0) {
                return 0L;
            }
            ResultSet resultSet = resultSetGroup.getResultSet(0);
            if (resultSet.getRowCount() == 0) {
                return 0L;
            }
            return resultSet.getLong(0, 0);
        } catch (PinotClientException ex) {
            log.warn("Pinot query failed for tenant={} campaign={} metric={}: {}",
                    tenantId, campaignId, metricType, ex.getMessage());
            return 0L;
        }
    }

    // ---- Reconciliation: grouped count query --------------------------------

    /**
     * Query Pinot for all (tenantId, campaignId, eventType) groups with their
     * event counts in the given epoch-millisecond window.
     *
     * <p>Used by the hourly and daily reconciliation jobs to discover active
     * campaigns and their authoritative counts without querying each key
     * individually.
     *
     * @param fromMs epoch ms (inclusive lower bound)
     * @param toMs   epoch ms (exclusive upper bound)
     * @param limit  maximum number of groups to return
     * @return list of campaign metric counts, empty if Pinot is disabled or errors
     */
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
            ResultSetGroup resultSetGroup = pinotConnection.execute(sql);
            return extractGroupedCounts(resultSetGroup);
        } catch (PinotClientException ex) {
            log.warn("[reconciliation] Pinot grouped-count query failed: {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

    // ---- Result extraction -----------------------------------------------

    /**
     * Parse a GROUP BY response into a list of {@link CampaignMetricCount}.
     *
     * <p>Expected column order (matching the SQL above):
     * {@code [tenant_id(0), campaign_id(1), event_type(2), count(*)(3)]}.
     */
    private static List<CampaignMetricCount> extractGroupedCounts(ResultSetGroup resultSetGroup) {
        if (resultSetGroup == null || resultSetGroup.getResultSetCount() == 0) {
            return Collections.emptyList();
        }
        ResultSet resultSet = resultSetGroup.getResultSet(0);
        List<CampaignMetricCount> results = new ArrayList<CampaignMetricCount>(resultSet.getRowCount());
        for (int i = 0; i < resultSet.getRowCount(); i++) {
            try {
                String tenantId   = resultSet.getString(i, 0);
                String campaignId = resultSet.getString(i, 1);
                String eventType  = resultSet.getString(i, 2);
                long   count      = resultSet.getLong(i, 3);
                results.add(new CampaignMetricCount(
                        new CampaignKey(tenantId, campaignId, eventType), count));
            } catch (Exception ex) {
                // Skip malformed rows rather than aborting the whole reconciliation run.
                log.debug("[reconciliation] Skipping malformed Pinot row at index {}: {}", i, ex.getMessage());
            }
        }
        return results;
    }

    /** Single-quote escaping: replace {@code '} with {@code ''} (SQL standard). */
    private static String escape(String value) {
        return value == null ? "" : value.replace("'", "''");
    }
}
