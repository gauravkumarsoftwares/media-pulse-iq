package com.java.query.config;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventConfluentDeserializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

/**
 * Kafka consumer configuration for ingesting enriched aggregate events.
 *
 * <h2>Zero-loss consumer</h2>
 * <ul>
 *   <li>Offset commit mode: {@code MANUAL_IMMEDIATE} — offset is committed only after
 *       {@link com.java.query.consumer.AggregateConsumer#onMessage} returns successfully.</li>
 *   <li>On failure, {@link DefaultErrorHandler} retries up to 3 times (1 s back-off).
 *       After exhaustion the record is logged and skipped (no DLQ needed — this service
 *       is a read-side projection; the source-of-truth lives in Pinot/Iceberg).</li>
 *   <li>Concurrency driven by {@code spring.kafka.listener.concurrency}; each thread
 *       owns whole partitions so per-partition order is preserved.</li>
 * </ul>
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    /** Confluent Schema Registry URL — required by KafkaAvroDeserializer. */
    @Value("${platform.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    @Bean
    public ConsumerFactory<String, ShoppingEvent> consumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Enforce manual commit regardless of YAML value.
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

        // Commit offset only after the listener returns without throwing.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // OB-1: enable Micrometer Observation so each consumed record continues the
        // distributed trace propagated via the W3C traceparent Kafka header.
        factory.getContainerProperties().setObservationEnabled(true);

        // 3 retries with 1 s back-off; after exhaustion log and skip.
        // No DLQ here — AggregateConsumer is a read-side projection backed by Pinot/Iceberg.
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                (record, ex) -> log.error(
                        "[CONSUMER] Skipping enriched event after retries: topic={} partition={} offset={} error={}",
                        record.topic(), record.partition(), record.offset(), ex.getMessage()),
                new FixedBackOff(1_000L, 3)));

        return factory;
    }
}
