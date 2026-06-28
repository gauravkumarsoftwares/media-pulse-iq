package com.java.query.reconciliation;

import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Thin scheduler that dispatches reconciliation runs to the appropriate
 * {@link ReconciliationStrategy} implementation (B1 SRP fix).
 *
 * <p>Adding a new reconciliation window requires only a new
 * {@link ReconciliationStrategy} bean — no changes here (OCP).
 */
@Component
@Slf4j
public class ReconciliationJob {

    private final ReconciliationProperties                   props;
    private final Map<ReconciliationWindow, ReconciliationStrategy> strategies;
    private final ReconciliationMetrics                      metrics;
    private final ReconciliationStore                        store;

    public ReconciliationJob(ReconciliationProperties props,
                              List<ReconciliationStrategy> strategyList,
                              ReconciliationMetrics metrics,
                              ReconciliationStore store) {
        this.props      = props;
        this.strategies = strategyList.stream().collect(
                Collectors.toUnmodifiableMap(ReconciliationStrategy::supportedWindow, s -> s));
        this.metrics    = metrics;
        this.store      = store;
    }

    @Scheduled(cron = "${platform.reconciliation.hourly-cron:0 5 * * * *}")
    public void runHourly() {
        if (!props.isEnabled()) {
            log.debug("[reconciliation] Skipping hourly run — reconciliation disabled");
            return;
        }
        log.info("[reconciliation] Starting hourly hot-tier reconciliation (Redis vs Pinot)");
        runAndStore(ReconciliationWindow.HOURLY);
    }

    @Scheduled(cron = "${platform.reconciliation.daily-cron:0 15 1 * * *}")
    public void runDaily() {
        if (!props.isEnabled()) {
            log.debug("[reconciliation] Skipping daily run — reconciliation disabled");
            return;
        }
        log.info("[reconciliation] Starting daily warm-tier reconciliation (Pinot vs Iceberg)");
        runAndStore(ReconciliationWindow.DAILY);
    }

    /**
     * Execute a reconciliation run for the given window and persist the report.
     * Called by scheduled methods and by {@link com.java.query.service.ReconciliationQueryServiceImpl}
     * for on-demand REST-triggered runs.
     */
    public ReconciliationReport runAndStore(ReconciliationWindow window) {
        Timer.Sample sample = metrics.startRun();
        Instant start = Instant.now();
        ReconciliationReport report;
        try {
            report = strategies.get(window).execute();
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

    private void logSummary(ReconciliationReport report) {
        log.info("[reconciliation][{}] run={} status={} campaigns={} discrepancies={} "
                        + "autoPatched={} elapsed={}ms",
                report.getWindow().getLabel(), report.getRunId(), report.getStatus(),
                report.totalCampaigns(), report.discrepancyCount(),
                report.autoPatchedCount(), report.getElapsed().toMillis());
    }
}
