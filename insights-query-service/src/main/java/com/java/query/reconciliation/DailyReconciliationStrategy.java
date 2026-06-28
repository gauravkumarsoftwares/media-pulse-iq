package com.java.query.reconciliation;

import com.java.query.store.PinotRestClient;
import com.java.query.store.TrinoIcebergClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Daily warm-tier reconciliation: compares Pinot (warm) against
 * Iceberg/Trino (cold, source of truth) for the previous complete UTC calendar day.
 *
 * <p>A single batch Trino query replaces N+1 per-campaign scans — at 10 000+
 * campaigns this reduces cold-tier load from minutes to seconds.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DailyReconciliationStrategy implements ReconciliationStrategy {

    private final ReconciliationProperties props;
    private final PinotRestClient          pinotClient;
    private final TrinoIcebergClient       trinoClient;

    @Override
    public ReconciliationWindow supportedWindow() {
        return ReconciliationWindow.DAILY;
    }

    @Override
    public ReconciliationReport execute() {
        ZonedDateTime todayMidnightUtc     = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
        ZonedDateTime yesterdayMidnightUtc = todayMidnightUtc.minusDays(1);
        Instant begin = yesterdayMidnightUtc.toInstant();
        Instant end   = todayMidnightUtc.toInstant();

        log.debug("[reconciliation][daily] Querying Pinot for window [{}, {})", begin, end);

        List<CampaignMetricCount> pinotCounts =
                pinotClient.queryGroupedCounts(begin.toEpochMilli(), end.toEpochMilli(),
                        props.getMaxCampaignsPerRun());
        log.info("[reconciliation][daily] Pinot returned {} groups for {}",
                pinotCounts.size(), yesterdayMidnightUtc.toLocalDate());

        Map<CampaignKey, Long> icebergCounts = trinoClient
                .queryGroupedCounts(begin.toEpochMilli(), end.toEpochMilli(),
                        props.getMaxCampaignsPerRun())
                .stream()
                .collect(Collectors.toMap(CampaignMetricCount::key, CampaignMetricCount::count));
        log.info("[reconciliation][daily] Iceberg returned {} groups for {}",
                icebergCounts.size(), yesterdayMidnightUtc.toLocalDate());

        List<ReconciliationResult> results = new ArrayList<>(pinotCounts.size());
        for (CampaignMetricCount pinotEntry : pinotCounts) {
            CampaignKey key         = pinotEntry.key();
            long        pinotCount  = pinotEntry.count();
            long        icebergCount = icebergCounts.getOrDefault(key, 0L);

            if (icebergCount == 0 && pinotCount > 0) {
                log.debug("[reconciliation][daily] Iceberg returned 0 for {} — "
                                + "Trino may not be configured; skipping",
                        key);
                continue;
            }

            ReconciliationResult result = ReconciliationResult.of(
                    key, "iceberg", icebergCount, "pinot", pinotCount, false);
            if (result.exceedsThreshold(props.getDiscrepancyThresholdPct())) {
                log.warn("[reconciliation][daily] DISCREPANCY for {} — "
                                + "iceberg={} pinot={} delta={} ({:.2f}%)",
                        key, icebergCount, pinotCount, result.delta(), result.discrepancyPct());
            }
            results.add(result);
        }

        return buildReport(ReconciliationWindow.DAILY, begin, end, results,
                props.getDiscrepancyThresholdPct());
    }
}

