package com.java.ingestion.dlq;

/**
 * Response body for {@code POST /api/v1/admin/dlq/replay}.
 *
 * @param replayed  number of DLQ events re-published to the raw topic
 * @param skipped   events filtered out by tenantId / reason criteria
 * @param failed    events that could not be re-published (transient broker error)
 * @param total     total DLQ events polled during this replay run
 */
public record DlqReplaySummary(
        int replayed,
        int skipped,
        int failed,
        int total
) {}

