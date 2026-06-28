package com.java.query.reconciliation;

import com.java.query.store.PinotRestClient;
import com.java.query.store.RedisInsightsStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Hourly hot-tier reconciliation: compares Redis hot counters against Pinot
 * counts for the last 2-hour window.
 *
 * <p>Auto-patches Redis deficits when
 * {@code platform.reconciliation.auto-correct-redis=true} by delegating the
 * increment to {@link RedisInsightsStore#incrementCount} (DIP — no direct
 * {@code StringRedisTemplate} dependency here).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class HourlyReconciliationStrategy implements ReconciliationStrategy {

    private final ReconciliationProperties props;
    private final PinotRestClient          pinotClient;
    private final RedisInsightsStore       redisStore;

    @Override
    public ReconciliationWindow supportedWindow() {
        return ReconciliationWindow.HOURLY;
    }

    @Override
    public ReconciliationReport execute() {
        Instant end   = Instant.now();
        Instant begin = end.minus(2, ChronoUnit.HOURS);

        List<CampaignMetricCount> pinotCounts =
                pinotClient.queryGroupedCounts(begin.toEpochMilli(), end.toEpochMilli(),
                        props.getMaxCampaignsPerRun());
        log.debug("[reconciliation][hourly] Pinot returned {} campaign-metric groups", pinotCounts.size());

        List<ReconciliationResult> results = new ArrayList<>(pinotCounts.size());
        for (CampaignMetricCount pinotEntry : pinotCounts) {
            CampaignKey key        = pinotEntry.key();
            long        pinotCount = pinotEntry.count();
            long        redisCount = redisStore.getCount(key.tenantId(), key.campaignId(),
                                                          key.eventType()).orElse(0L);
            boolean patched = false;

            if (redisCount < pinotCount && props.isAutoCorrectRedis()) {
                long deficit = pinotCount - redisCount;
                try {
                    redisStore.incrementCount(key.tenantId(), key.campaignId(),
                            key.eventType(), deficit);
                    patched = true;
                    log.info("[reconciliation][hourly] Auto-patched Redis for {} — "
                                    + "delta={} (redis={}, pinot={})",
                            key, deficit, redisCount, pinotCount);
                } catch (Exception ex) {
                    log.warn("[reconciliation][hourly] Failed to patch Redis for {}: {}",
                            key, ex.getMessage());
                }
            } else if (redisCount > pinotCount) {
                log.warn("[reconciliation][hourly] Redis OVER-COUNT for {} — "
                                + "redis={} > pinot={} (delta={}). "
                                + "Possible inflight events; no auto-patch applied.",
                        key, redisCount, pinotCount, redisCount - pinotCount);
            }

            results.add(ReconciliationResult.of(
                    key, "pinot", pinotCount, "redis", redisCount, patched));
        }

        return buildReport(ReconciliationWindow.HOURLY, begin, end, results,
                props.getDiscrepancyThresholdPct());
    }
}

