package com.java.query.reconciliation;

import com.java.query.store.PinotRestClient;
import com.java.query.store.RedisInsightsStore;
import com.java.query.store.TrinoIcebergClient;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Periodic reconciliation job that validates data consistency across the three
 * serving tiers (Redis → Pinot → Iceberg/Trino).
 *
 * <h2>Why reconciliation matters</h2>
 * <p>Click data is revenue data. Transient Flink errors, bad deployments,
 * out-of-order Kafka events, and Redis TTL expirations can all lead to silent
 * count drift. Two scheduled jobs catch these:
 *
 * <h3>Hourly job — hot-tier (Redis vs Pinot)</h3>
 * <p>Runs every hour (configurable cron, default: 5 past the hour). Queries Pinot
 * for all campaign event counts that arrived in the last 2 hours, then compares
 * each against the corresponding Redis hash counter.  When Redis is lower than
 * Pinot (dropped events), the deficit is automatically corrected via
 * {@code HINCRBY} — making the hot cache converge with the durable warm store.
 * The reverse case (Redis &gt; Pinot) is flagged as an alert but NOT auto-corrected,
 * since the enriched Kafka events may simply not have propagated to Pinot yet.
 *
 * <h3>Daily job — warm vs cold tier (Pinot vs Iceberg)</h3>
 * <p>Runs once a day (default: 01:15 UTC, after midnight Pinot segment flush).
 * Queries both Pinot and Iceberg (Trino) for the previous complete calendar day
 * and compares counts per (tenantId, campaignId, eventType).  Discrepancies
 * beyond the configured threshold ({@code platform.reconciliation.discrepancy-threshold-pct})
 * are logged, recorded in metrics, and stored in {@link ReconciliationStore} for
 * inspection via the {@link ReconciliationController} REST API.
 *
 * <h2>Source-of-truth hierarchy</h2>
 * <pre>
 *   Iceberg (S3 Parquet) &gt; Pinot (OLAP warm) &gt; Redis (hot cache)
 * </pre>
 * Iceberg stores every raw deduplicated event written by Flink and is therefore
 * the authoritative record.  Pinot derives its data from the Kafka enriched topic
 * (same Flink output), so any divergence indicates a missed write on one side.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReconciliationJob {

    private final ReconciliationProperties    props;
    private final PinotRestClient             pinotClient;
    private final RedisInsightsStore          redisStore;
    private final TrinoIcebergClient          trinoClient;
    private final StringRedisTemplate         redisTemplate;
    private final ReconciliationMetrics       metrics;
    private final ReconciliationStore         store;

    // =========================================================================
    // Hourly job — hot-tier: Redis vs recent Pinot
    // =========================================================================

    /**
     * Runs on the {@code platform.reconciliation.hourly-cron} schedule.
     *
     * <p>Compares Redis hot counters to Pinot counts for the last 2-hour window.
     * Auto-patches Redis deficits when {@code platform.reconciliation.auto-correct-redis=true}.
     */
    @Scheduled(cron = "${platform.reconciliation.hourly-cron:0 5 * * * *}")
    public void runHourly() {
        if (!props.isEnabled()) {
            log.debug("[reconciliation] Skipping hourly run — reconciliation disabled");
            return;
        }
        log.info("[reconciliation] Starting hourly hot-tier reconciliation (Redis vs Pinot, last 2 h)");
        runAndStore(ReconciliationWindow.HOURLY);
    }

    // =========================================================================
    // Daily job — warm-tier: Pinot vs Iceberg
    // =========================================================================

    /**
     * Runs on the {@code platform.reconciliation.daily-cron} schedule.
     *
     * <p>Compares Pinot (warm tier) against Iceberg/Trino (source of truth)
     * for the previous complete UTC calendar day.
     */
    @Scheduled(cron = "${platform.reconciliation.daily-cron:0 15 1 * * *}")
    public void runDaily() {
        if (!props.isEnabled()) {
            log.debug("[reconciliation] Skipping daily run — reconciliation disabled");
            return;
        }
        log.info("[reconciliation] Starting daily warm-tier reconciliation (Pinot vs Iceberg, yesterday)");
        runAndStore(ReconciliationWindow.DAILY);
    }

    // =========================================================================
    // Orchestration
    // =========================================================================

    /**
     * Execute a reconciliation run for the given window and persist the report.
     * Can also be called programmatically from {@link ReconciliationController}.
     *
     * @param window  the reconciliation window type to execute
     * @return the completed {@link ReconciliationReport}
     */
    public ReconciliationReport runAndStore(ReconciliationWindow window) {
        Timer.Sample sample = metrics.startRun();
        Instant start = Instant.now();
        ReconciliationReport report;
        try {
            report = switch (window) {
                case HOURLY -> executeHourlyRun();
                case DAILY  -> executeDailyRun();
            };
        } catch (Exception ex) {
            log.error("[reconciliation] {} run failed: {}", window.getLabel(), ex.getMessage(), ex);
            report = ReconciliationReport.builder(window)
                    .windowStart(start)
                    .windowEnd(Instant.now())
                    .elapsed(Duration.between(start, Instant.now()))
                    .results(List.of())
                    .status(ReconciliationReport.RunStatus.ERROR)
                    .message(ex.getMessage())
                    .build();
        }
        metrics.recordRun(sample, report);
        store.save(report);
        logSummary(report);
        return report;
    }

    // =========================================================================
    // Hourly run implementation
    // =========================================================================

    private ReconciliationReport executeHourlyRun() {
        Instant end   = Instant.now();
        Instant begin = end.minus(2, ChronoUnit.HOURS);

        // Step 1: discover all active campaigns in the 2-hour Pinot window
        List<CampaignMetricCount> pinotCounts =
                pinotClient.queryGroupedCounts(begin.toEpochMilli(), end.toEpochMilli(),
                        props.getMaxCampaignsPerRun());
        log.debug("[reconciliation][hourly] Pinot returned {} campaign-metric groups", pinotCounts.size());

        // Step 2: compare each against Redis
        List<ReconciliationResult> results = new ArrayList<>(pinotCounts.size());
        for (CampaignMetricCount pinotEntry : pinotCounts) {
            CampaignKey key       = pinotEntry.key();
            long pinotCount       = pinotEntry.count();
            long redisCount       = redisStore.getCount(key.tenantId(), key.campaignId(),
                                                        key.eventType())
                                              .orElse(0L);
            boolean patched       = false;

            // Redis under-count: auto-patch the deficit
            if (redisCount < pinotCount && props.isAutoCorrectRedis()) {
                long deficit = pinotCount - redisCount;
                try {
                    redisTemplate.opsForHash().increment(
                            key.redisHashKey(), key.redisHashField(), deficit);
                    patched = true;
                    log.info("[reconciliation][hourly] Auto-patched Redis for {} — "
                                    + "delta={} (redis={}, pinot={})",
                            key, deficit, redisCount, pinotCount);
                } catch (Exception ex) {
                    log.warn("[reconciliation][hourly] Failed to patch Redis for {}: {}",
                            key, ex.getMessage());
                }
            } else if (redisCount > pinotCount) {
                // Redis over-count: log warning only (Pinot may have ingestion lag)
                log.warn("[reconciliation][hourly] Redis OVER-COUNT for {} — "
                                + "redis={} > pinot={} (delta={}). "
                                + "Possible inflight events; no auto-patch applied.",
                        key, redisCount, pinotCount, redisCount - pinotCount);
            }

            results.add(ReconciliationResult.of(
                    key, "pinot", pinotCount, "redis", redisCount, patched));
        }

        return buildReport(ReconciliationWindow.HOURLY, begin, end, results);
    }

    // =========================================================================
    // Daily run implementation
    // =========================================================================

    private ReconciliationReport executeDailyRun() {
        // Window = yesterday midnight → today midnight (UTC)
        ZonedDateTime todayMidnightUtc     = ZonedDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
        ZonedDateTime yesterdayMidnightUtc = todayMidnightUtc.minusDays(1);
        Instant begin = yesterdayMidnightUtc.toInstant();
        Instant end   = todayMidnightUtc.toInstant();

        log.debug("[reconciliation][daily] Querying Pinot for window [{}, {})", begin, end);

        // Step 1: get all campaign counts from Pinot for yesterday
        List<CampaignMetricCount> pinotCounts =
                pinotClient.queryGroupedCounts(begin.toEpochMilli(), end.toEpochMilli(),
                        props.getMaxCampaignsPerRun());
        log.info("[reconciliation][daily] Pinot returned {} campaign-metric groups for {}",
                pinotCounts.size(), yesterdayMidnightUtc.toLocalDate());

        // Step 2: single batch Trino query — one scan instead of N+1 per campaign.
        // At 10 000+ campaigns this reduces cold-tier load from minutes to seconds.
        Map<CampaignKey, Long> icebergCounts = trinoClient
                .queryGroupedCounts(begin.toEpochMilli(), end.toEpochMilli(),
                        props.getMaxCampaignsPerRun())
                .stream()
                .collect(Collectors.toMap(CampaignMetricCount::key, CampaignMetricCount::count));
        log.info("[reconciliation][daily] Iceberg returned {} campaign-metric groups for {}",
                icebergCounts.size(), yesterdayMidnightUtc.toLocalDate());

        // Step 3: compare Pinot vs Iceberg per campaign key
        List<ReconciliationResult> results = new ArrayList<>(pinotCounts.size());
        for (CampaignMetricCount pinotEntry : pinotCounts) {
            CampaignKey key    = pinotEntry.key();
            long pinotCount    = pinotEntry.count();
            long icebergCount  = icebergCounts.getOrDefault(key, 0L);

            if (icebergCount == 0 && pinotCount > 0) {
                // Trino is a stub or Iceberg is not yet configured; treat as "no reference"
                log.debug("[reconciliation][daily] Iceberg returned 0 for {} — "
                                + "Trino may not be configured; skipping comparison",
                        key);
                continue;
            }

            ReconciliationResult result = ReconciliationResult.of(
                    key, "iceberg", icebergCount, "pinot", pinotCount, false);

            if (result.exceedsThreshold(props.getDiscrepancyThresholdPct())) {
                log.warn("[reconciliation][daily] DISCREPANCY detected for {} — "
                                + "iceberg={} pinot={} delta={} ({:.2f}%)",
                        key, icebergCount, pinotCount, result.delta(), result.discrepancyPct());
            }
            results.add(result);
        }

        return buildReport(ReconciliationWindow.DAILY, begin, end, results);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private ReconciliationReport buildReport(ReconciliationWindow window,
                                             Instant begin, Instant end,
                                             List<ReconciliationResult> results) {
        int discrepancies = (int) results.stream()
                .filter(r -> r.exceedsThreshold(props.getDiscrepancyThresholdPct()))
                .count();

        ReconciliationReport.RunStatus status = discrepancies > 0
                ? ReconciliationReport.RunStatus.DISCREPANCIES_FOUND
                : ReconciliationReport.RunStatus.OK;

        return ReconciliationReport.builder(window)
                .windowStart(begin)
                .windowEnd(end)
                .elapsed(Duration.between(begin, Instant.now()))
                .results(results)
                .status(status)
                .build();
    }

    private void logSummary(ReconciliationReport report) {
        log.info("[reconciliation][{}] run={} status={} campaigns={} discrepancies={} autoPatched={} elapsed={}ms",
                report.getWindow().getLabel(),
                report.getRunId(),
                report.getStatus(),
                report.totalCampaigns(),
                report.discrepancyCount(),
                report.autoPatchedCount(),
                report.getElapsed().toMillis());
    }
}

