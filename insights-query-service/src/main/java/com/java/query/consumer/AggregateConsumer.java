package com.java.query.consumer;

import com.java.model.ShoppingEvent;
import com.java.query.store.StarTreeStore;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Consumes enriched and attributed events from the DDD-named enriched topic and
 * indexes them into the local OLAP store that backs the read APIs.
 *
 * <p>Offset is committed manually ({@link Acknowledgment#acknowledge()}) only after
 * {@link StarTreeStore#index} succeeds, guaranteeing no event is skipped silently.
 * On failure the container's {@code DefaultErrorHandler} retries 3 times before logging
 * and skipping (read-side projection — source-of-truth is in Pinot/Iceberg).
 *
 * <p><strong>Observability (OB-2)</strong>: extracts the {@code X-Request-Id} Kafka
 * header (propagated from the ingestion-service) and restores it in MDC so all log lines
 * emitted during indexing carry the original HTTP correlation ID.
 * Both MDC keys are cleared in the {@code finally} block to prevent thread-pool leaks.
 *
 * Topic: ${platform.kafka.topic.enriched}
 * (e.g. prod.internal.event.ads.attribution.ad-interaction-enriched-by-campaign)
 */
@Component
@RequiredArgsConstructor
public final class AggregateConsumer {

    private final StarTreeStore store;

    @KafkaListener(topics = "${platform.kafka.topic.enriched}",
            groupId = "${platform.kafka.consumer-group.insights-serving}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, ShoppingEvent> record, Acknowledgment ack) {
        ShoppingEvent event = record.value();

        // OB-2: restore HTTP correlation ID in MDC for end-to-end log tracing.
        Header requestIdHeader = record.headers().lastHeader("X-Request-Id");
        MDC.put("requestId", requestIdHeader != null
                ? new String(requestIdHeader.value(), StandardCharsets.UTF_8)
                : "no-correlation-id");
        MDC.put("tenantId", event != null ? event.getTenantId() : "unknown");

        try {
            store.index(event);
            // Commit offset only after indexing succeeds.
            // If store.index() throws, DefaultErrorHandler retries before skipping.
            ack.acknowledge();
        } finally {
            MDC.remove("requestId");
            MDC.remove("tenantId");
        }
    }
}
