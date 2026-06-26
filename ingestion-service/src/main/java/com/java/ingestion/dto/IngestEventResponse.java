package com.java.ingestion.dto;

/**
 * Response envelope for a successfully accepted ingest request.
 *
 * @param status             always {@code "ACCEPTED"}
 * @param eventId            the event identifier (echo of request or server-assigned)
 * @param processedTimestamp ISO-8601 instant at which the event was accepted
 * @param remainingQuota     remaining per-tenant rate-limit quota in the current window
 */
public record IngestEventResponse(
        String status,
        String eventId,
        String processedTimestamp,
        long remainingQuota) {}

