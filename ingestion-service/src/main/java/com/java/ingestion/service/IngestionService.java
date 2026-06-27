package com.java.ingestion.service;

import com.java.ingestion.dto.IngestEventRequest;
import com.java.ingestion.dto.IngestEventResponse;

/**
 * Write-path service contract (CQRS, architecture 2).
 *
 * <p>Isolates the controller from all business logic: rate limiting, schema
 * validation, DLQ routing, and Kafka publish. Implementations throw
 * {@link com.java.ingestion.common.ApiException} for any recoverable failure.
 */
public interface IngestionService {

    /**
     * Validate, rate-limit, and publish an ingest event.
     *
     * @param request   the validated ingest payload (Jakarta constraints already enforced)
     * @param tenantId  the verified tenant extracted from {@code X-Tenant-Context}
     * @return response containing the acceptance confirmation and remaining quota
     */
    IngestEventResponse ingest(IngestEventRequest request, String tenantId);
}

