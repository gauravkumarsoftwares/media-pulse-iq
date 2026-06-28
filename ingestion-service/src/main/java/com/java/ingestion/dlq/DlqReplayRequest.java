package com.java.ingestion.dlq;

/**
 * Request body for {@code POST /api/v1/admin/dlq/replay}.
 *
 * <p>All filter fields are optional — omitting them replays ALL events in the DLQ
 * partition up to {@code maxEvents}.
 *
 * @param tenantId   only replay events matching this tenant (null = all tenants)
 * @param reason     only replay events whose {@code dlq-reason} header starts with this
 *                   prefix (null = all reasons)
 * @param maxEvents  upper bound on the number of events to replay per invocation (default 1000)
 */
public record DlqReplayRequest(
        String tenantId,
        String reason,
        int maxEvents
) {
    public DlqReplayRequest {
        if (maxEvents <= 0) maxEvents = 1000;
        if (maxEvents > 50_000) maxEvents = 50_000;   // safety cap
    }
}

