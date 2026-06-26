package com.java.query.reconciliation;

/**
 * Granularity of a single reconciliation run.
 *
 * <ul>
 *   <li>{@link #HOURLY} — hot-tier check: Redis hot counters vs recent Pinot counts (last 2 h).</li>
 *   <li>{@link #DAILY}  — warm-tier check: Pinot counts vs Iceberg/Trino source-of-truth for
 *       the previous complete calendar day.</li>
 * </ul>
 */
public enum ReconciliationWindow {

    /** Redis vs Pinot comparison for the last 2 hours. */
    HOURLY("hourly", 2),

    /** Pinot vs Iceberg comparison for the previous calendar day (24 hours). */
    DAILY("daily", 24);

    private final String label;
    /** Width of the comparison window in hours. */
    private final int windowHours;

    ReconciliationWindow(String label, int windowHours) {
        this.label = label;
        this.windowHours = windowHours;
    }

    public String getLabel()       { return label; }
    public int    getWindowHours() { return windowHours; }
}

