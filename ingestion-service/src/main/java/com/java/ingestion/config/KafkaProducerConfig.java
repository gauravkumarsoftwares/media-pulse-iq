package com.java.ingestion.config;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventConfluentSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

/**
 * Configures the Kafka producer used by the write path.
 *
 * <p>Value serialiser: {@link ShoppingEventConfluentSerializer} — delegates to
 * Confluent {@code KafkaAvroSerializer}. Wire format: Confluent Avro binary
 * {@code [0x00][4-byte schemaId][Avro binary]}.
 * Schema registered/looked-up via {@code platform.schema-registry.url} (2.1).
 *
 * <p>All connection + <strong>security</strong> settings (SASL_SSL / mTLS) are
 * sourced from {@link KafkaProperties} ({@code spring.kafka.*}) — no code change
 * required per environment (OWASP A02).
 */
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaProperties kafkaProperties;

    /** Confluent Schema Registry URL — required by KafkaAvroSerializer. */
    @Value("${platform.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    @Bean
    public ProducerFactory<String, ShoppingEvent> producerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ShoppingEventConfluentSerializer.class);
        // Schema Registry endpoint — picked up by KafkaAvroSerializer inside
        // ShoppingEventConfluentSerializer.configure().
        props.put("schema.registry.url", schemaRegistryUrl);

        // ---- Zero-loss: idempotent producer (architecture 2.4) ---------------
        // Prevents broker-side duplicate writes on retry.
        // Requires acks=all (set in application.yml) and max.in.flight <= 5.
        // Kafka client validates this combination at startup — fail-fast on mis-config.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,              true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,  5);
        // Effectively infinite retries within the delivery window.
        props.putIfAbsent(ProducerConfig.RETRIES_CONFIG,               Integer.MAX_VALUE);
        props.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,   120_000);   // 2 min
        props.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,    30_000);    // per-attempt

        // ---- Throughput: batching + compression ------------------------------
        // Wait up to 10 ms to accumulate a fuller batch before sending.
        props.putIfAbsent(ProducerConfig.LINGER_MS_CONFIG,      10);
        // 64 KB max batch size per partition.
        props.putIfAbsent(ProducerConfig.BATCH_SIZE_CONFIG,     65_536);
        // 64 MB total in-flight buffer; producer blocks after this (back-pressure).
        props.putIfAbsent(ProducerConfig.BUFFER_MEMORY_CONFIG,  67_108_864L);
        // LZ4 gives ~50–60 % size reduction on Avro payloads with minimal CPU cost.
        props.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ShoppingEvent> kafkaTemplate() {
        KafkaTemplate<String, ShoppingEvent> template = new KafkaTemplate<>(producerFactory());
        // OB-1: enable Micrometer Observation so each send creates a tracing span and
        // injects the W3C traceparent header into every ProducerRecord.
        template.setObservationEnabled(true);
        return template;
    }
}
