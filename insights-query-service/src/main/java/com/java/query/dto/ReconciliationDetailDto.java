package com.java.query.dto;

import java.util.List;

/**
 * Full reconciliation report including per-campaign comparison results.
 * Returned by the {@code GET .../latest} and {@code POST .../runs/{window}} endpoints.
 */
public record ReconciliationDetailDto(
        String runId,
        String status,
        String runTime,
        String windowStart,
        String windowEnd,
        int campaigns,
        int discrepancies,
        int autoPatched,
        long elapsedMs,
        String message,
        List<ReconciliationResultDto> results) {}

