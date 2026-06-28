package com.java.query.dto;

/**
 * Common summary fields shared by status and paginated-list responses for a
 * reconciliation run (A1 — merges the former {@code ReconciliationStatusEntry}
 * and {@code ReconciliationSummaryDto} which were byte-for-byte identical records).
 *
 * <p>Used by:
 * <ul>
 *   <li>{@code GET /api/v1/reconciliation/status} — latest run per window</li>
 *   <li>{@code GET /api/v1/reconciliation/reports/{window}} — paginated run list</li>
 * </ul>
 *
 * @param runId          unique run identifier (UUID); {@code null} if no run yet
 * @param status         run outcome: {@code OK}, {@code DISCREPANCIES_FOUND},
 *                       {@code ERROR}, or {@code NO_RUN_YET}
 * @param runTime        ISO-8601 wall-clock time of the run
 * @param windowStart    ISO-8601 start of the compared time window
 * @param windowEnd      ISO-8601 end of the compared time window
 * @param campaigns      total campaigns compared
 * @param discrepancies  campaigns that breached the configured threshold
 * @param autoPatched    campaigns auto-corrected in Redis
 * @param elapsedMs      run duration in milliseconds
 * @param message        optional error/info message (populated on {@code ERROR} status)
 */
public record ReconciliationRunSummary(
        String runId,
        String status,
        String runTime,
        String windowStart,
        String windowEnd,
        int    campaigns,
        int    discrepancies,
        int    autoPatched,
        long   elapsedMs,
        String message) {}

