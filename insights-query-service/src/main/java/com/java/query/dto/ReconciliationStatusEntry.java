package com.java.query.dto;

/**
 * Lightweight summary of the most recent reconciliation run for one window type.
 * Used by the {@code GET /api/v1/reconciliation/status} endpoint.
 *
 * @param runId          unique run identifier (UUID)
 * @param status         run outcome ({@code OK}, {@code DISCREPANCIES_FOUND}, {@code ERROR},
 *                       or {@code NO_RUN_YET})
 * @param runTime        ISO-8601 wall-clock time of the run
 * @param windowStart    ISO-8601 start of the compared time window (null if no run yet)
 * @param windowEnd      ISO-8601 end of the compared time window (null if no run yet)
 * @param campaigns      total campaigns compared
 * @param discrepancies  campaigns that breached the threshold
 * @param autoPatched    campaigns auto-corrected in Redis
 * @param elapsedMs      run duration in milliseconds
 * @param message        optional error message (present on {@code ERROR} status)
 */
public record ReconciliationStatusEntry(
        String runId,
        String status,
        String runTime,
        String windowStart,
        String windowEnd,
        int campaigns,
        int discrepancies,
        int autoPatched,
        long elapsedMs,
        String message) {}

