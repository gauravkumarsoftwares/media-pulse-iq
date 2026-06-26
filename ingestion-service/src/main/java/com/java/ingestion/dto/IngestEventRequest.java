package com.java.ingestion.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * Strongly-typed ingest request DTO with Jakarta Bean Validation constraints.
 * The controller binds the JSON body to this record and validates it via
 * {@code @Valid} before the request reaches the service layer.
 *
 * <p>Separation of concern: this DTO is the API surface contract; the domain
 * model ({@link com.java.model.ShoppingEvent}) is the internal Kafka wire contract.
 */
public record IngestEventRequest(

        @NotBlank(message = "eventId is required")
        @Size(max = 128, message = "eventId must not exceed 128 characters")
        String eventId,

        @NotBlank(message = "userId is required")
        @Size(max = 128, message = "userId must not exceed 128 characters")
        String userId,

        @NotBlank(message = "sessionId is required")
        @Size(max = 128, message = "sessionId must not exceed 128 characters")
        String sessionId,

        @Size(max = 128, message = "campaignId must not exceed 128 characters")
        String campaignId,

        @NotBlank(message = "eventType is required")
        String eventType,

        long eventTimestampMs,

        @DecimalMin(value = "0.0", message = "cost must be non-negative")
        double cost,

        Map<String, String> customTags
) {}

