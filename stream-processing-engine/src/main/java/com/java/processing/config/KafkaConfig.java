package com.java.processing.config;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventConfluentDeserializer;
import com.java.model.avro.ShoppingEventConfluentSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

/**
 * Kafka consumer (raw topic) and producer (aggregates topic) configuration for the
 * <em>Spring-Kafka fallback path</em> ({@code platform.flink.enabled=false}).
 *
 * <h2>Zero-loss consumer</h2>
 * <ul>
 *   <li>Offset commit mode: {@code MANUAL_IMMEDIATE} — offset is committed only after
 *       {@link com.java.processing.consumer.EventConsumer#onMessage} completes successfully.
 *       If the listener throws, the offset is NOT committed and {@link DefaultErrorHandler}
 *       retries up to 3 times (1 s back-off). After all retries fail the event is sent to
 *       the DLQ ({@code platform.kafka.topic.dlq}) for SRE replay.</li>
 *   <li>{@code enable.auto.commit=false} is enforced in code regardless of YAML.</li>
 *   <li>Concurrency is driven by {@code spring.kafka.listener.concurrency}; each thread
 *       owns whole partitions so per-partition order is preserved.</li>
 * </ul>
 *
 * <h2>Zero-loss producer</h2>
 * <p>Idempotent producer ({@code enable.idempotence=true}) prevents broker-side duplicates
 * on retry.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    /** Confluent Schema Registry URL — required by KafkaAvroSerializer/Deserializer. */
    @Value("${platform.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    /** DLQ topic for events that fail after all consumer retries are exhausted. */
    @Value("${platform.kafka.topic.dlq}")
    private String dlqTopic;

    // ---- Consumer (raw topic) -----------------------------------------------

    @Bean
    public ConsumerFactory<String, ShoppingEvent> consumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Enforce manual commit — critical for zero-loss.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("schema.registry.url", schemaRegistryUrl);
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ShoppingEventConfluentDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ShoppingEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ShoppingEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Each thread owns whole partitions — per-partition order is preserved.
        Integer concurrency = kafkaProperties.getListener().getConcurrency();
        if (concurrency != null && concurrency > 0) {
            factory.setConcurrency(concurrency);
        }

        // Commit offset only after the listener returns successfully.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // OB-1: enable Micrometer Observation so each consumed record continues the
        // distributed trace from the W3C traceparent header injected by the producer.
        factory.getContainerProperties().setObservationEnabled(true);

        // 3 retries with 1 s back-off; after exhaustion route to DLQ.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate(),
                (record, ex) -> {
                    log.error("[CONSUMER] Sending failed event to DLQ: topic={} partition={} offset={} error={}",
                            record.topic(), record.partition(), record.offset(), ex.getMessage());
                    return new TopicPartition(dlqTopic, -1);  // -1 = round-robin partition
                });
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3)));

        return factory;
    }

    // ---- Producer (aggregates topic + DLQ) ----------------------------------

    @Bean
    public ProducerFactory<String, ShoppingEvent> producerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ShoppingEventConfluentSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);

        // ---- Zero-loss: idempotent producer ---------------------------------
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,             true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG,              Integer.MAX_VALUE);
        props.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,  120_000);
        props.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,   30_000);
        // ---- Throughput: batching + LZ4 compression -------------------------
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG,                "all");
        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG,           10);
        props.putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG,          65_536);
        props.putIfAbsent(ProducerConfig.BUFFER_MEMORY_CONFIG,       67_108_864L);
        props.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG,    "lz4");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ShoppingEvent> kafkaTemplate() {
        KafkaTemplate<String, ShoppingEvent> template = new KafkaTemplate<>(producerFactory());
        // OB-1: enable tracing observation on the producer side too.
        template.setObservationEnabled(true);
        return template;
    }
}



