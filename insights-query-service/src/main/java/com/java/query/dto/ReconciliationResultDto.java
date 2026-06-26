package com.java.query.dto;

/**
 * Individual campaign-metric comparison result within a reconciliation report.
 */
public record ReconciliationResultDto(
        String tenantId,
        String campaignId,
        String eventType,
        String referenceStore,
        long referenceCount,
        String observedStore,
        long observedCount,
        long delta,
        String discrepancyPct,
        boolean autoPatched) {}

