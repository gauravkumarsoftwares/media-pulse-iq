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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Core stream pipeline: consume raw → deduplicate → stateful join →
 * sink enriched events to the serving layer (architecture 3).
 *
 * <p><strong>Fallback mode only.</strong> This Spring-Kafka consumer is active
 * when {@code platform.flink.enabled=false} (local / dev).
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
    public void onMessage(ShoppingEvent event) {
        if (event == null) {
            return;
        }

        Timer.Sample sample = metrics.startTimer();

        // 1. Deduplicate (drop replays / double-clicks).
        if (!deduplicator.isUnique(event.getEventId(), event.getEventTimestampMs())) {
            log.debug("Dropped duplicate event {}", event.getEventId());
            metrics.recordDedupDropped(event.getTenantId());
            metrics.setDedupStateSize(deduplicator.size());
            return;
        }

        // 2. Forward unique event to the serving layer (Pinot / Redis hot counters).
        pinotSink.upsert(event);
        metrics.recordProcessed(event.getTenantId(), event.getEventType());

        // 3. Append raw event to Iceberg data lake (1: Flink → Append Row-level Events → Iceberg).
        //    Provides source-of-truth for cold queries (Trino >30d) + billing reconciliation.
        icebergSink.append(event);

        // 4. Stateful attribution: emit synthesized CLICK_TO_BASKET when matched.
        ShoppingEvent conversion = statefulJoiner.join(event);
        if (conversion != null) {
            log.info("Attributed conversion: session={} campaign={}",
                    conversion.getSessionId(), conversion.getCampaignId());
            pinotSink.upsert(conversion);
            icebergSink.append(conversion);   // also archive the attributed conversion
            metrics.recordAttributed(event.getTenantId(), conversion.getCampaignId());
        }

        metrics.stopTimer(sample, event.getTenantId(), event.getEventType());
        metrics.setDedupStateSize(deduplicator.size());
    }
}
