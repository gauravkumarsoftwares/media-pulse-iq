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
        props.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ShoppingEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
