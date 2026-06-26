package com.java.query.config;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventConfluentDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Map;

/**
 * Kafka consumer configuration for ingesting enriched aggregate events.
 *
 * <p>Wire format: <strong>Confluent Avro binary</strong>
 * {@code [0x00][4-byte schemaId][Avro binary]}.
 * Value deserialiser: {@link ShoppingEventConfluentDeserializer} — delegates to
 * Confluent {@code KafkaAvroDeserializer}. Schema looked-up from Schema Registry
 * ({@code platform.schema-registry.url}, §2.1).
 *
 * <p>Security (SASL_SSL / mTLS) is driven by {@code spring.kafka.properties.*}
 * via {@link KafkaProperties} (OWASP A02).
 */
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    /** Confluent Schema Registry URL — required by KafkaAvroDeserializer. */
    @Value("${platform.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    @Bean
    public ConsumerFactory<String, ShoppingEvent> consumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
        return factory;
    }
}
