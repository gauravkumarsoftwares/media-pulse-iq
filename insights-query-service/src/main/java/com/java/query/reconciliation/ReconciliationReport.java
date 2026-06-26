package com.java.query.reconciliation;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Aggregated report produced by a single reconciliation run.
 *
 * <p>Instances are immutable and are stored in {@link ReconciliationStore} for
 * retrieval via the {@link ReconciliationController} REST API.
 */
public final class ReconciliationReport {

    /** Unique identifier for this run (random UUID). */
    private final String runId;

    /** Which type of reconciliation window this report covers. */
    private final ReconciliationWindow window;

    /** Inclusive start of the comparison time window. */
    private final Instant windowStart;

    /** Exclusive end of the comparison time window. */
    private final Instant windowEnd;

    /** Wall-clock time when this run was triggered. */
    private final Instant runTime;

    /** Time taken for the full run. */
    private final Duration elapsed;

    /** Individual campaign comparison results. */
    private final List<ReconciliationResult> results;

    /** High-level run outcome. */
    private final RunStatus status;

    /** Optional message (error description when {@code status == ERROR}). */
    private final String message;

    public enum RunStatus { OK, DISCREPANCIES_FOUND, ERROR }

    // ---- constructor -------------------------------------------------------

    private ReconciliationReport(Builder b) {
        this.runId       = b.runId;
        this.window      = b.window;
        this.windowStart = b.windowStart;
        this.windowEnd   = b.windowEnd;
        this.runTime     = b.runTime;
        this.elapsed     = b.elapsed;
        this.results     = List.copyOf(b.results);
        this.status      = b.status;
        this.message     = b.message;
    }

    // ---- accessors ---------------------------------------------------------

    public String                    getRunId()       { return runId; }
    public ReconciliationWindow      getWindow()      { return window; }
    public Instant                   getWindowStart() { return windowStart; }
    public Instant                   getWindowEnd()   { return windowEnd; }
    public Instant                   getRunTime()     { return runTime; }
    public Duration                  getElapsed()     { return elapsed; }
    public List<ReconciliationResult> getResults()    { return results; }
    public RunStatus                 getStatus()      { return status; }
    public String                    getMessage()     { return message; }

    // ---- derived stats -----------------------------------------------------

    public int totalCampaigns()    { return results.size(); }
    public int discrepancyCount()  { return (int) results.stream().filter(r -> r.delta() != 0).count(); }
    public int autoPatchedCount()  { return (int) results.stream().filter(ReconciliationResult::autoPatched).count(); }

    // ---- builder -----------------------------------------------------------

    public static Builder builder(ReconciliationWindow window) {
        return new Builder(window);
    }

    public static final class Builder {
        private final String runId   = UUID.randomUUID().toString();
        private final ReconciliationWindow window;
        private Instant windowStart;
        private Instant windowEnd;
        private final Instant runTime = Instant.now();
        private Duration elapsed     = Duration.ZERO;
        private List<ReconciliationResult> results = List.of();
        private RunStatus status     = RunStatus.OK;
        private String message       = "";

        private Builder(ReconciliationWindow window) { this.window = window; }

        public Builder windowStart(Instant v) { this.windowStart = v; return this; }
        public Builder windowEnd(Instant v)   { this.windowEnd   = v; return this; }
        public Builder elapsed(Duration v)    { this.elapsed     = v; return this; }
        public Builder results(List<ReconciliationResult> v) { this.results = v; return this; }
        public Builder status(RunStatus v)    { this.status      = v; return this; }
        public Builder message(String v)      { this.message     = v; return this; }

        public ReconciliationReport build() { return new ReconciliationReport(this); }
    }
}

