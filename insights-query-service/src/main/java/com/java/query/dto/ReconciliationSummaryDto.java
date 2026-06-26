package com.java.query.dto;

/**
 * Compact report summary returned in paginated list responses.
 */
public record ReconciliationSummaryDto(
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

