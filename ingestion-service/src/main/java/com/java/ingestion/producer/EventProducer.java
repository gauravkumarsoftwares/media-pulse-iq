package com.java.ingestion.producer;

import com.java.model.ShoppingEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Publishes validated events onto the raw ad-interaction Kafka topic.
 *
 * <p>Topic: {@code ${platform.kafka.topic.raw}}
 * (resolved at runtime to e.g.
 * {@code prod.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session}).
 *
 * <p>Partition key = tenantId:sessionId so that all events of a single user session
 * land on the same partition, enabling the downstream stream engine to perform local,
 * zero-shuffle stateful joins (section 2.3).
 *
 * <p>Zero-loss guarantee: the Kafka producer is configured as idempotent
 * ({@code enable.idempotence=true}) so retries cannot create duplicates.
 * If the broker permanently rejects the message (delivery timeout exceeded),
 * the event is routed to the DLQ via {@link DlqProducer} for SRE replay.
 *
 * <p>Observability (OB-2): the current MDC {@code requestId} is injected as a
 * Kafka record header ({@code X-Request-Id}) so that downstream consumers can
 * restore the correlation ID in their own MDC context, enabling end-to-end
 * log correlation across the async Kafka boundary.
 *
 * <p>Observability (OB-1): Spring Kafka observation
 * ({@code spring.kafka.template.observation-enabled=true}) automatically creates a
 * Micrometer Observation span for each send and injects the W3C {@code traceparent}
 * header, propagating the distributed trace to the stream-processing-engine.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public final class EventProducer {

    private final KafkaTemplate<String, ShoppingEvent> kafkaTemplate;
    private final DlqProducer dlqProducer;

    /** Resolved at startup from the DDD-named topic property. */
    @Value("${platform.kafka.topic.raw}")
    private String topicRaw;

    /**
     * Asynchronously publish an event, keyed for session-ordered partitioning.
     * Injects {@code X-Request-Id} Kafka header for cross-service log correlation.
     * On permanent broker failure the event is forwarded to the DLQ so no data is lost.
     *
     * @param event the validated, tenant-aligned shopping event
     */
    public void publish(ShoppingEvent event) {
        String partitionKey = event.getTenantId() + ":" + event.getSessionId();

        // Build a ProducerRecord to attach correlation headers (OB-2).
        ProducerRecord<String, ShoppingEvent> record = new ProducerRecord<>(topicRaw, partitionKey, event);

        // Carry the HTTP request correlation ID across the async Kafka boundary.
        String requestId = MDC.get("requestId");
        if (requestId != null) {
            record.headers().add("X-Request-Id", requestId.getBytes(StandardCharsets.UTF_8));
        }

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                // Idempotent producer already retried within delivery.timeout.ms.
                // Route to DLQ so the event survives for SRE replay.
                log.error("[PRODUCER] Permanent send failure — routing event {} to DLQ: {}",
                        event.getEventId(), ex.getMessage());
                dlqProducer.sendToDlq(event, "kafka-send-failure: " + ex.getMessage());
            } else if (result != null) {
                log.debug("[PRODUCER] Published event {} to partition {}",
                        event.getEventId(), result.getRecordMetadata().partition());
            }
        });
    }
}
