package com.java.ingestion.service;

import com.java.ingestion.common.ApiException;
import com.java.ingestion.dto.IngestEventRequest;
import com.java.ingestion.dto.IngestEventResponse;
import com.java.ingestion.observability.IngestionMetrics;
import com.java.ingestion.producer.DlqProducer;
import com.java.ingestion.producer.EventProducer;
import com.java.ingestion.ratelimit.TenantRateLimiter;
import com.java.ingestion.validation.SchemaValidator;
import com.java.model.ShoppingEvent;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

/**
 * Default {@link IngestionService} implementation.
 *
 * <p>Orchestrates the full write path: rate limiting → schema validation →
 * DLQ routing (on failure) → Kafka publish → metrics recording.
 *
 * <p>Business-rule failures are surfaced as {@link ApiException}s with explicit
 * HTTP statuses so the controller stays thin and the global handler formats them
 * uniformly.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionServiceImpl implements IngestionService {

    private final EventProducer    eventProducer;
    private final DlqProducer      dlqProducer;
    private final SchemaValidator  schemaValidator;
    private final IngestionMetrics metrics;
    private final TenantRateLimiter rateLimiter;

    @Override
    public IngestEventResponse ingest(IngestEventRequest request, String tenantId) {
        Timer.Sample sample = metrics.startTimer();

        // Per-tenant rate limiting (architecture §6.2 — noisy-neighbour protection)
        if (!rateLimiter.isAllowed(tenantId)) {
            metrics.stopTimer(sample, tenantId, "rate_limited");
            throw new ApiException(HttpStatus.TOO_MANY_REQUESTS,
                    "Rate limit exceeded. Reduce request frequency.");
        }

        // Map DTO → domain model for schema validation and Kafka publish.
        ShoppingEvent candidate = toEvent(request, tenantId);

        // Schema / business-rule validation (may route to DLQ).
        Optional<String> validationError = schemaValidator.validate(candidate);
        if (validationError.isPresent()) {
            dlqProducer.sendToDlq(candidate, validationError.get());
            metrics.recordValidationFailure(tenantId, validationError.get());
            metrics.recordDlq(tenantId, validationError.get());
            metrics.stopTimer(sample, tenantId, "validation_failed");
            throw new ApiException(HttpStatus.BAD_REQUEST, validationError.get());
        }

        eventProducer.publish(candidate);
        metrics.recordAccepted(tenantId, candidate.getEventType());
        metrics.stopTimer(sample, tenantId, "accepted");

        return new IngestEventResponse(
                "ACCEPTED",
                candidate.getEventId(),
                Instant.now().toString(),
                rateLimiter.remainingQuota(tenantId));
    }

    // ---- mapping -----------------------------------------------------------

    private static ShoppingEvent toEvent(IngestEventRequest r, String tenantId) {
        return ShoppingEvent.builder()
                .eventId(r.eventId())
                .tenantId(tenantId)
                .userId(r.userId())
                .sessionId(r.sessionId())
                .campaignId(r.campaignId())
                .eventType(r.eventType())
                .eventTimestampMs(r.eventTimestampMs() == 0
                        ? System.currentTimeMillis() : r.eventTimestampMs())
                .cost(r.cost())
                .customTags(r.customTags())
                .build();
    }
}

