package com.java.ingestion.producer;

import com.java.model.ShoppingEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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
 */
@Component
@Slf4j
@RequiredArgsConstructor
public final class EventProducer {

    private final KafkaTemplate<String, ShoppingEvent> kafkaTemplate;

    /** Resolved at startup from the DDD-named topic property. */
    @Value("${platform.kafka.topic.raw}")
    private String topicRaw;

    /**
     * Asynchronously publish an event, keyed for session-ordered partitioning.
     *
     * @param event the validated, tenant-aligned shopping event
     */
    public void publish(ShoppingEvent event) {
        String partitionKey = event.getTenantId() + ":" + event.getSessionId();
        kafkaTemplate.send(topicRaw, partitionKey, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event {}: {}", event.getEventId(), ex.getMessage());
            } else if (result != null) {
                log.debug("Published event {} to partition {}", event.getEventId(),
                        result.getRecordMetadata().partition());
            }
        });
    }
}
