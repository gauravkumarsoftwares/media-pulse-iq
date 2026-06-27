package com.java.processing.job;

import com.java.model.ShoppingEvent;
import com.java.processing.config.FlinkProperties;
import com.java.processing.operator.AttributionJoinFunction;
import com.java.processing.operator.DeduplicationFunction;
import com.java.processing.operator.ShoppingEventDeserializationSchema;
import com.java.processing.operator.ShoppingEventSerializationSchema;
import com.java.processing.sink.RedisHotCounterSink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Apache Flink DataStream job — the production stream-processing topology
 * (architecture 3).
 *
 * <p>Pipeline:
 * <pre>
 *  KafkaSource (events.shopping.raw)
 *      │
 *      ▼  keyBy(eventId)
 *  DeduplicationFunction  ── RocksDB ValueState + TTL ──► drops duplicates
 *      │ unique events
 *      ├──► KafkaSink (events.shopping.enriched)   ← Pinot RealtimeTable ingests from here
 *      ├──► RedisHotCounterSink                    ← hot counter HINCRBY (<48h tier)
 *      │
 *      └──  keyBy(sessionId)
 *           AttributionJoinFunction  ── ValueState + ProcessingTimeTimer ──► CLICK_TO_BASKET
 *               │ conversions
 *               ├──► KafkaSink (events.shopping.enriched)
 *               └──► RedisHotCounterSink
 * </pre>
 *
 * <p>This Spring component is instantiated (and {@link #execute()} called from a daemon
 * thread by {@link com.java.processing.config.FlinkJobLauncher}) only when
 * {@code platform.flink.enabled=true}.
 */
@Component
@ConditionalOnProperty(name = "platform.flink.enabled", havingValue = "true")
@RequiredArgsConstructor
@Slf4j
public class FlinkStreamingJob {

    private final FlinkProperties flinkProperties;

    /** Falls back to spring.kafka.bootstrap-servers if the Flink-specific override is blank. */
    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String springKafkaBootstrapServers;

    /** DDD-named raw topic: {@code {env}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session} */
    @Value("${platform.kafka.topic.raw}")
    private String topicRaw;

    /** DDD-named enriched topic: {@code {env}.internal.event.ads.attribution.ad-interaction-enriched-by-campaign} */
    @Value("${platform.kafka.topic.enriched}")
    private String topicEnriched;

    /** DDD-named consumer group: {@code {env}.ads.stream-processing-engine} */
    @Value("${platform.kafka.consumer-group.stream-engine}")
    private String consumerGroup;

    @Value("${platform.dedup.ttl-minutes:60}")
    private long dedupTtlMinutes;

    @Value("${platform.attribution.window-hours:24}")
    private long attributionWindowHours;

    /**
     * Builds and executes the Flink streaming topology. This method blocks the
     * calling thread until the job finishes or is cancelled (use a daemon thread).
     */
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(flinkProperties.getParallelism());

        configureStateBackend(env);
        configureCheckpointing(env);

        String bootstrapServers = resolveBootstrapServers();
        log.info("Building Flink topology → Kafka={}, parallelism={}",
                bootstrapServers, flinkProperties.getParallelism());

        // ---- Source -------------------------------------------------------
        KafkaSource<ShoppingEvent> kafkaSource = KafkaSource.<ShoppingEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topicRaw)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(
                        org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST))
                .setDeserializer(new ShoppingEventDeserializationSchema(
                        flinkProperties.getSchemaRegistryUrl()))
                .build();

        DataStream<ShoppingEvent> rawStream = env
                // Architecture 3.3: bounded out-of-orderness watermark (5 min tolerance)
                // allows Flink to process events up to 5 minutes late without dropping them.
                // Events arriving later than 5 min are routed to a side output / DLQ.
                .fromSource(kafkaSource,
                        WatermarkStrategy.<ShoppingEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
                                .withTimestampAssigner(
                                        (event, ts) -> event.getEventTimestampMs()),
                        "kafka-raw-source")
                .uid("kafka-raw-source");

        // ---- Deduplication (keyed by eventId) --------------------------------
        DataStream<ShoppingEvent> uniqueStream = rawStream
                .keyBy(ShoppingEvent::getEventId)
                .process(new DeduplicationFunction(dedupTtlMinutes))
                .name("dedup-events")
                .uid("dedup-events");

        // ---- Pinot real-time ingestion sink ----------------------------------
        KafkaSink<ShoppingEvent> pinotEnrichedSink = buildKafkaSink(bootstrapServers,
                topicEnriched);

        // All unique raw events → Pinot (CLICK, IMPRESSION, ADD_TO_CART, …)
        uniqueStream
                .sinkTo(pinotEnrichedSink)
                .name("pinot-raw-sink")
                .uid("pinot-raw-sink");

        // ---- Redis hot-counter sink ------------------------------------------
        RedisHotCounterSink redisSink = buildRedisSink();

        uniqueStream
                .addSink(redisSink)
                .name("redis-raw-counter-sink")
                .uid("redis-raw-counter-sink");

        // ---- Sessionized attribution join (keyed by sessionId) ---------------
        DataStream<ShoppingEvent> conversions = uniqueStream
                .keyBy(ShoppingEvent::getSessionId)
                .process(new AttributionJoinFunction(attributionWindowHours))
                .name("attribution-join")
                .uid("attribution-join");

        // Attributed CLICK_TO_BASKET events → Pinot
        conversions
                .sinkTo(pinotEnrichedSink)
                .name("pinot-conversion-sink")
                .uid("pinot-conversion-sink");

        // Attributed CLICK_TO_BASKET events → Redis hot counter
        conversions
                .addSink(redisSink)
                .name("redis-conversion-counter-sink")
                .uid("redis-conversion-counter-sink");

        log.info("Flink topology built — executing job 'ShoppingEventStreamJob'");
        env.execute("ShoppingEventStreamJob");
    }

    // ---- Private helpers ---------------------------------------------------

    private void configureStateBackend(StreamExecutionEnvironment env) {
        if (flinkProperties.isUseRocksDb()) {
            // Incremental RocksDB checkpoints — essential for large state (billions of events).
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
            log.info("Flink state backend: EmbeddedRocksDB (incremental)");
        } else {
            log.info("Flink state backend: default in-memory (dev/local)");
        }
    }

    private void configureCheckpointing(StreamExecutionEnvironment env) {
        if (flinkProperties.getCheckpointIntervalMs() <= 0) {
            log.info("Flink checkpointing disabled");
            return;
        }

        env.enableCheckpointing(flinkProperties.getCheckpointIntervalMs(),
                CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setCheckpointTimeout(60_000L);
        cc.setMinPauseBetweenCheckpoints(5_000L);
        cc.setMaxConcurrentCheckpoints(1);
        cc.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        cc.setTolerableCheckpointFailureNumber(3);

        if (!flinkProperties.getCheckpointDir().isBlank()) {
            cc.setCheckpointStorage(
                    new FileSystemCheckpointStorage(flinkProperties.getCheckpointDir()));
            log.info("Flink checkpoint storage: {}", flinkProperties.getCheckpointDir());
        }
    }

    private String resolveBootstrapServers() {
        String override = flinkProperties.getKafkaBootstrapServers();
        return (override != null && !override.isBlank()) ? override : springKafkaBootstrapServers;
    }

    private KafkaSink<ShoppingEvent> buildKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<ShoppingEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        (KafkaRecordSerializationSchema<ShoppingEvent>)
                                new ShoppingEventSerializationSchema(
                                        topic, flinkProperties.getSchemaRegistryUrl()))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private RedisHotCounterSink buildRedisSink() {
        return new RedisHotCounterSink(
                flinkProperties.getRedisHost(),
                flinkProperties.getRedisPort(),
                flinkProperties.getRedisPassword(),
                flinkProperties.getRedisDatabase(),
                flinkProperties.getRedisTtlSeconds());
    }
}






