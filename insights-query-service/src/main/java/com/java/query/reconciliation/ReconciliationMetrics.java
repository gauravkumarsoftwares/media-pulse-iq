package com.java.query.reconciliation;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metrics for the reconciliation pipeline.
 *
 * <h2>Metrics emitted</h2>
 * <pre>
 *  ads.reconciliation.runs.total{window}              Counter  total completed runs by window type
 *  ads.reconciliation.runs.errors{window}             Counter  runs that ended in ERROR status
 *  ads.reconciliation.discrepancies{window,store}     Counter  campaigns flagged with a discrepancy
 *  ads.reconciliation.auto.patches{window}            Counter  Redis keys auto-corrected
 *  ads.reconciliation.latency{window}                 Timer    wall-clock time per run
 *  ads.reconciliation.campaigns.checked               Gauge    campaigns checked in the last run
 * </pre>
 */
@Component
@RequiredArgsConstructor
public class ReconciliationMetrics {

    private final MeterRegistry registry;

    // ---- metric name constants --------------------------------------------

    public static final String RUNS_TOTAL         = "ads.reconciliation.runs.total";
    public static final String RUNS_ERRORS        = "ads.reconciliation.runs.errors";
    public static final String DISCREPANCIES      = "ads.reconciliation.discrepancies";
    public static final String AUTO_PATCHES       = "ads.reconciliation.auto.patches";
    public static final String RUN_LATENCY        = "ads.reconciliation.latency";
    public static final String CAMPAIGNS_CHECKED  = "ads.reconciliation.campaigns.checked";

    // Gauge backing value (last run only)
    private final AtomicLong lastCampaignsChecked = new AtomicLong(0);

    // ---- public API -------------------------------------------------------

    /** Start a latency sample before executing a reconciliation run. */
    public Timer.Sample startRun() {
        return Timer.start(registry);
    }

    /**
     * Record metrics for a completed reconciliation run.
     *
     * @param sample   the sample returned by {@link #startRun()}
     * @param report   the completed report
     */
    public void recordRun(Timer.Sample sample, ReconciliationReport report) {
        String windowLabel = report.getWindow().getLabel();

        // Latency
        sample.stop(Timer.builder(RUN_LATENCY)
                .description("Wall-clock time for a reconciliation run")
                .tag("window", windowLabel)
                .register(registry));

        // Run counters
        counter(RUNS_TOTAL, "window", windowLabel).increment();
        if (report.getStatus() == ReconciliationReport.RunStatus.ERROR) {
            counter(RUNS_ERRORS, "window", windowLabel).increment();
        }

        // Discrepancy counters — split by which store was observed
        long discrepancyCount = report.getResults().stream()
                .filter(r -> r.delta() != 0)
                .count();
        if (discrepancyCount > 0) {
            // Determine the observed store from the first result (all results in a run share the same stores)
            String observedStore = report.getResults().stream()
                    .filter(r -> r.delta() != 0)
                    .findFirst()
                    .map(ReconciliationResult::observedStore)
                    .orElse("unknown");
            counter(DISCREPANCIES, "window", windowLabel, "store", observedStore)
                    .increment(discrepancyCount);
        }

        // Auto-patch counter
        long patched = report.autoPatchedCount();
        if (patched > 0) {
            counter(AUTO_PATCHES, "window", windowLabel).increment(patched);
        }

        // Update the gauge
        lastCampaignsChecked.set(report.totalCampaigns());
        Gauge.builder(CAMPAIGNS_CHECKED, lastCampaignsChecked, AtomicLong::get)
                .description("Number of campaigns checked in the most recent reconciliation run")
                .register(registry);
    }

    // ---- helpers ----------------------------------------------------------

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }
}

