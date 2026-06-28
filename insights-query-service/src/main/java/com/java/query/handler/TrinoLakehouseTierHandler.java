package com.java.query.handler;

import com.java.query.dto.TimeSeriesPoint;
import com.java.query.store.TrinoIcebergClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Cold-tier handler: issues native Trino SQL over Apache Iceberg / S3 Parquet.
 *
 * <p>The StarTree fallback has been removed (TRINO-FB fix): {@code resolveCount}
 * now returns the raw Trino result and {@code resolveTimeSeries} returns the real
 * Trino-bucketed series or an empty list with a WARN log. Empty results are no
 * longer silently masked — they surface in monitoring so connectivity issues are
 * visible instead of hidden.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TrinoLakehouseTierHandler implements TierQueryHandler {

    /** Default look-back when {@code from} is null — matches Trino tier activation boundary. */
    private static final int DEFAULT_COLD_WINDOW_DAYS = 31;

    private final TrinoIcebergClient trinoClient;

    @Override
    public long resolveCount(String tenantId, String campaignId,
                             String metricType, Instant from) {
        Instant trinoTo   = Instant.now();
        Instant trinoFrom = (from != null) ? from
                : trinoTo.minus(DEFAULT_COLD_WINDOW_DAYS, ChronoUnit.DAYS);

        long count = trinoClient.queryCount(tenantId, campaignId, metricType, trinoFrom, trinoTo);
        log.debug("[trino] resolveCount tenant={} campaign={} metric={} [{},{}] → {}",
                tenantId, campaignId, metricType, trinoFrom, trinoTo, count);
        return count;
    }

    @Override
    public List<TimeSeriesPoint> resolveTimeSeries(String tenantId, String campaignId,
                                                    String metricType,
                                                    Instant from, Instant to, String grain) {
        List<TimeSeriesPoint> series =
                trinoClient.queryTimeSeries(tenantId, campaignId, metricType, from, to, grain);

        if (series.isEmpty()) {
            log.warn("[trino] resolveTimeSeries returned empty series — "
                    + "Trino may not be configured or no data exists for "
                    + "tenant={} campaign={} metric={} grain={}",
                    tenantId, campaignId, metricType, grain);
        }
        return series;
    }
}
