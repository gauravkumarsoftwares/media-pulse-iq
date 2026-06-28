package com.java.ingestion.controller;

import com.java.ingestion.dto.IngestEventRequest;
import com.java.ingestion.dto.IngestEventResponse;
import com.java.ingestion.service.IngestionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Edge ingestion REST controller — CQRS Write Path (architecture 2).
 *
 * <p>URI: {@code POST /api/v1/events}. Deliberately thin: it extracts the
 * tenant identity and delegates all business logic to {@link IngestionService}.
 * Input is validated automatically via {@code @Valid} on the DTO before the
 * service is ever called; validation failures return {@code 422} via
 * {@link com.java.ingestion.common.GlobalExceptionHandler}.
 */
@RestController
@RequestMapping("/api/v1/events")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Events", description = "Ad-interaction event ingestion")
public final class IngestController {

    private final IngestionService ingestionService;

    /**
     * Accept a single ad-interaction event and dispatch it to the Kafka stream.
     *
     * @param request       the validated event payload
     * @param tenantContext the tenant identifier injected by the gateway (X-Tenant-Context)
     */
    @PostMapping
    @Operation(summary = "Ingest an ad-interaction event",
               description = "Accepts a validated event, applies per-tenant rate limiting, and publishes to Kafka.")
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "Event accepted and dispatched"),
            @ApiResponse(responseCode = "401", description = "Missing tenant context"),
            @ApiResponse(responseCode = "403", description = "Missing write:events scope"),
            @ApiResponse(responseCode = "422", description = "Request validation failed"),
            @ApiResponse(responseCode = "429", description = "Tenant rate limit exceeded")
    })
    public ResponseEntity<IngestEventResponse> ingest(
            @Valid @RequestBody IngestEventRequest request,
            @RequestHeader(value = "X-Tenant-Context", required = false) String tenantContext) {

        log.info("[API] POST /api/v1/events tenant={} eventId={} eventType={}",
                tenantContext, request.eventId(), request.eventType());

        if (tenantContext == null || tenantContext.isBlank()) {
            log.warn("[API] POST /api/v1/events rejected — missing X-Tenant-Context header");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        IngestEventResponse response = ingestionService.ingest(request, tenantContext);
        log.info("[API] POST /api/v1/events accepted tenant={} eventId={} remainingQuota={}",
                tenantContext, response.eventId(), response.remainingQuota());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
