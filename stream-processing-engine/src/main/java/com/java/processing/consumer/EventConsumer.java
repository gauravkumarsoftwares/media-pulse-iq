package com.java.processing.consumer;

import com.java.model.ShoppingEvent;
import com.java.processing.cleaner.Deduplicator;
import com.java.processing.join.StatefulJoiner;
import com.java.processing.observability.ProcessingMetrics;
import com.java.processing.sink.PinotSink;
import com.java.processing.sink.IcebergS3Sink;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Core stream pipeline: consume raw → deduplicate → stateful join →
 * sink enriched events to the serving layer (architecture 3).
 *
 * <p><strong>Fallback mode only.</strong> This Spring-Kafka consumer is active
 * when {@code platform.flink.enabled=false} (local / dev).
 *
 * <p><strong>Observability (OB-2)</strong>: extracts the {@code X-Request-Id} Kafka
 * header (injected by {@link com.java.ingestion.producer.EventProducer}) and restores
 * it in MDC so every log line emitted during processing carries the original HTTP
 * correlation ID. Also sets {@code tenantId} for tenant-scoped log filtering.
 * Both MDC keys are cleared in the {@code finally} block.
 *
 * <p><strong>Observability (OB-1)</strong>: the Micrometer Observation enabled in
 * {@link com.java.processing.config.KafkaConfig} automatically extracts the W3C
 * {@code traceparent} header and continues the distributed trace without code changes.
 */
@Component
@ConditionalOnProperty(name = "platform.flink.enabled", havingValue = "false", matchIfMissing = true)
@Slf4j
@RequiredArgsConstructor
public final class EventConsumer {

    private final Deduplicator deduplicator;
    private final StatefulJoiner statefulJoiner;
    private final PinotSink pinotSink;
    private final IcebergS3Sink icebergSink;   // data-lake write path (architecture 1, 3.3)
    private final ProcessingMetrics metrics;


    @KafkaListener(topics = "${platform.kafka.topic.raw}",
            groupId = "${platform.kafka.consumer-group.stream-engine}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ConsumerRecord<String, ShoppingEvent> record, Acknowledgment ack) {
        ShoppingEvent event = record.value();
        if (event == null) {
            ack.acknowledge();   // null record: skip cleanly, advance offset
            return;
        }

        // OB-2: restore HTTP correlation ID in MDC for end-to-end log tracing.
        Header requestIdHeader = record.headers().lastHeader("X-Request-Id");
        String requestId = requestIdHeader != null
                ? new String(requestIdHeader.value(), StandardCharsets.UTF_8)
                : "no-correlation-id";
        MDC.put("requestId", requestId);
        MDC.put("tenantId", event.getTenantId());

        try {
            Timer.Sample sample = metrics.startTimer();

            // 1. Deduplicate (drop replays / double-clicks).
            if (!deduplicator.isUnique(event.getEventId(), event.getEventTimestampMs())) {
                log.debug("Dropped duplicate event {}", event.getEventId());
                metrics.recordDedupDropped(event.getTenantId());
                metrics.setDedupStateSize(deduplicator.size());
                ack.acknowledge();   // duplicate: safe to advance offset
                return;
            }

            // 2. Forward unique event to the serving layer (Pinot / Redis hot counters).
            pinotSink.upsert(event);
            metrics.recordProcessed(event.getTenantId(), event.getEventType());

            // 3. Append raw event to Iceberg data lake (1: Flink → Append Row-level Events → Iceberg).
            icebergSink.append(event);

            // 4. Stateful attribution: emit synthesized CLICK_TO_BASKET when matched.
            ShoppingEvent conversion = statefulJoiner.join(event);
            if (conversion != null) {
                log.info("Attributed conversion: session={} campaign={}",
                        conversion.getSessionId(), conversion.getCampaignId());
                pinotSink.upsert(conversion);
                icebergSink.append(conversion);
                metrics.recordAttributed(event.getTenantId(), conversion.getCampaignId());
            }

            metrics.stopTimer(sample, event.getTenantId(), event.getEventType());
            metrics.setDedupStateSize(deduplicator.size());

            // Commit offset only after all sinks succeed.
            ack.acknowledge();
        } finally {
            MDC.remove("requestId");
            MDC.remove("tenantId");
        }
    }
}
