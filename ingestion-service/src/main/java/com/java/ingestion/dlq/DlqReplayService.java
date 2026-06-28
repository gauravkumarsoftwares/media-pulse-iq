package com.java.ingestion.dlq;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventConfluentDeserializer;
import com.java.ingestion.producer.EventProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * DLQ replay service — reads from the dead letter queue and re-publishes events
 * to the raw topic for reprocessing (OB-4).
 *
 * <h2>Design</h2>
 * <ul>
 *   <li>Uses a one-shot {@link KafkaConsumer} per invocation with a unique group ID
 *       so replays never interfere with the live consumer offsets.</li>
 *   <li>Seeks to the beginning of all DLQ partitions; reads up to {@code maxEvents}.</li>
 *   <li>Filters by {@code tenantId} (Kafka record key) and/or {@code dlq-reason}
 *       header prefix if specified in the request.</li>
 *   <li>Re-publishes matching events via {@link EventProducer#publish} which preserves
 *       the {@code tenantId:sessionId} partition key for ordering guarantees.</li>
 *   <li>Increments {@link DlqReplayMetrics} counters after the run.</li>
 * </ul>
 *
 * <h2>Stats</h2>
 * <p>The {@link #getStats()} method uses {@link AdminClient} to return current DLQ
 * consumer-group lag and partition watermark info without consuming any records.
 */
@Service
@Slf4j
public class DlqReplayService {

    private final EventProducer eventProducer;
    private final KafkaAdmin kafkaAdmin;
    private final DlqReplayMetrics metrics;

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${platform.kafka.topic.dlq}")
    private String dlqTopic;

    @Value("${platform.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    public DlqReplayService(EventProducer eventProducer,
                             KafkaAdmin kafkaAdmin,
                             DlqReplayMetrics metrics) {
        this.eventProducer = eventProducer;
        this.kafkaAdmin   = kafkaAdmin;
        this.metrics      = metrics;
    }

    /**
     * Replay DLQ events matching the given criteria back to the raw topic.
     *
     * @param request filter criteria and maximum event count
     * @return summary of replayed / skipped / failed counts
     */
    public DlqReplaySummary replay(DlqReplayRequest request) {
        log.info("[DLQ-REPLAY] Starting replay: tenantId={} reason={} maxEvents={}",
                request.tenantId(), request.reason(), request.maxEvents());

        int replayed = 0, skipped = 0, failed = 0, total = 0;

        try (KafkaConsumer<String, ShoppingEvent> consumer = createOneOffConsumer()) {
            // Assign all partitions for the DLQ topic.
            List<TopicPartition> partitions = getPartitions(consumer);
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            outer:
            while (total < request.maxEvents()) {
                ConsumerRecords<String, ShoppingEvent> records =
                        consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) break;   // end of topic reached

                for (ConsumerRecord<String, ShoppingEvent> record : records) {
                    if (total >= request.maxEvents()) break outer;
                    total++;

                    // Filter by tenantId (record key).
                    if (request.tenantId() != null
                            && !request.tenantId().equalsIgnoreCase(record.key())) {
                        skipped++;
                        continue;
                    }

                    // Filter by dlq-reason header prefix.
                    if (request.reason() != null) {
                        String reason = headerValue(record, "dlq-reason");
                        if (reason == null || !reason.startsWith(request.reason())) {
                            skipped++;
                            continue;
                        }
                    }

                    // Re-publish to raw topic.
                    ShoppingEvent event = record.value();
                    if (event == null) {
                        skipped++;
                        continue;
                    }

                    try {
                        eventProducer.publish(event);
                        replayed++;
                        log.debug("[DLQ-REPLAY] Re-queued eventId={} tenantId={}",
                                event.getEventId(), event.getTenantId());
                    } catch (Exception ex) {
                        failed++;
                        log.error("[DLQ-REPLAY] Failed to re-queue eventId={}: {}",
                                event.getEventId(), ex.getMessage());
                    }
                }
            }
        }

        metrics.recordReplayed(replayed);
        metrics.recordFailed(failed);
        log.info("[DLQ-REPLAY] Completed: total={} replayed={} skipped={} failed={}",
                total, replayed, skipped, failed);
        return new DlqReplaySummary(replayed, skipped, failed, total);
    }

    /**
     * Returns current DLQ partition watermarks (earliest / latest offsets) as a
     * diagnostic map: {@code partitionId → {earliest, latest, lag}}.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("dlqTopic", dlqTopic);

        try (AdminClient admin = AdminClient.create(
                Map.of("bootstrap.servers", bootstrapServers))) {

            // List latest offsets per partition.
            try (KafkaConsumer<String, ShoppingEvent> consumer = createOneOffConsumer()) {
                List<TopicPartition> partitions = getPartitions(consumer);
                Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
                Map<TopicPartition, OffsetSpec> earliestRequest = new HashMap<>();
                partitions.forEach(tp -> {
                    latestRequest.put(tp, OffsetSpec.latest());
                    earliestRequest.put(tp, OffsetSpec.earliest());
                });

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                        admin.listOffsets(latestRequest).all().get();
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
                        admin.listOffsets(earliestRequest).all().get();

                long totalLag = 0;
                List<Map<String, Object>> partitionStats = new ArrayList<>();
                for (TopicPartition tp : partitions) {
                    long latest   = latestOffsets.getOrDefault(tp, emptyResult()).offset();
                    long earliest = earliestOffsets.getOrDefault(tp, emptyResult()).offset();
                    long lag      = Math.max(0, latest - earliest);
                    totalLag += lag;
                    partitionStats.add(Map.of(
                            "partition", tp.partition(),
                            "earliest",  earliest,
                            "latest",    latest,
                            "lag",       lag
                    ));
                }
                stats.put("totalLag", totalLag);
                stats.put("partitions", partitionStats);
            }
        } catch (Exception ex) {
            log.warn("[DLQ-STATS] Failed to retrieve DLQ stats: {}", ex.getMessage());
            stats.put("error", ex.getMessage());
        }
        return stats;
    }

    // ---- Helpers ---------------------------------------------------------------

    private KafkaConsumer<String, ShoppingEvent> createOneOffConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Unique group ID so replay reads don't advance live consumer offsets.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-replay-admin-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ShoppingEventConfluentDeserializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return new KafkaConsumer<>(props);
    }

    private List<TopicPartition> getPartitions(KafkaConsumer<String, ShoppingEvent> consumer) {
        return consumer.partitionsFor(dlqTopic).stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .toList();
    }

    private String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        Header h = record.headers().lastHeader(headerName);
        return h != null ? new String(h.value(), StandardCharsets.UTF_8) : null;
    }

    private ListOffsetsResult.ListOffsetsResultInfo emptyResult() {
        return new ListOffsetsResult.ListOffsetsResultInfo(-1L, -1L, Optional.empty());
    }
}



