package com.java.query.reconciliation;

import com.java.query.reconciliation.ReconciliationReport;
import com.java.query.reconciliation.ReconciliationResult;
import com.java.query.reconciliation.ReconciliationWindow;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Strategy contract for a single reconciliation window (B1 — SRP split).
 *
 * <p>Each implementation owns the full comparison logic for one window type
 * ({@link ReconciliationWindow#HOURLY} or {@link ReconciliationWindow#DAILY}).
 * The {@link ReconciliationJob} is a thin scheduler that dispatches to the
 * right strategy — adding a new window requires only a new implementation,
 * no changes to the job (OCP).
 */
public interface ReconciliationStrategy {

    /** The window type this strategy handles. */
    ReconciliationWindow supportedWindow();

    /**
     * Execute the reconciliation logic and return a completed report.
     * Implementations must not throw; they should return an ERROR-status
     * report on failure (the job wraps execution in try/catch as well).
     */
    ReconciliationReport execute();

    /**
     * Helper shared by all strategies: build a {@link ReconciliationReport}
     * from the collected results.  Computes status and elapsed time automatically.
     *
     * @param window        the window type
     * @param begin         comparison window start
     * @param end           comparison window end
     * @param results       per-campaign comparison results
     * @param thresholdPct  discrepancy percentage threshold for status escalation
     */
    default ReconciliationReport buildReport(ReconciliationWindow window,
                                             Instant begin, Instant end,
                                             List<ReconciliationResult> results,
                                             double thresholdPct) {
        int discrepancies = (int) results.stream()
                .filter(r -> r.exceedsThreshold(thresholdPct))
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
}

