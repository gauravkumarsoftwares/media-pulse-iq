package com.java.ingestion.validation;

import com.java.model.EventType;
import com.java.model.ShoppingEvent;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Lightweight schema validator standing in for the Confluent Schema Registry
 * check described in §2.2. Enforces presence of mandatory identity fields and
 * a recognized event type before an event is admitted to the stream.
 */
@Component
public final class SchemaValidator {

    /**
     * Validate the payload.
     *
     * @param event the candidate event
     * @return an {@link Optional} error message; empty when the event is valid
     */
    public Optional<String> validate(ShoppingEvent event) {
        if (event == null) {
            return Optional.of("Event payload is null");
        }
        if (isBlank(event.getEventId())) {
            return Optional.of("Missing required field: eventId");
        }
        if (isBlank(event.getUserId())) {
            return Optional.of("Missing required field: userId");
        }
        if (isBlank(event.getSessionId())) {
            return Optional.of("Missing required field: sessionId");
        }
        if (EventType.from(event.getEventType()) == EventType.UNSPECIFIED) {
            return Optional.of("Unrecognized or missing eventType: " + event.getEventType());
        }
        return Optional.empty();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
