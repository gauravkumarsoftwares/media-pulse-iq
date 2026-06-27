package com.java.processing.config;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventConfluentDeserializer;
import com.java.model.avro.ShoppingEventConfluentSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import java.util.Map;

/**
 * Kafka consumer (raw topic) and producer (aggregates topic) configuration for the
 * <em>Spring-Kafka fallback path</em> ({@code platform.flink.enabled=false}).
 *
 * <p>Wire format: <strong>Confluent Avro binary</strong>
 * {@code [0x00][4-byte schemaId][Avro binary]}.
 * Consumer: {@link ShoppingEventConfluentDeserializer};
 * producer: {@link ShoppingEventConfluentSerializer}.
 * Both delegate to the Confluent {@code KafkaAvroDeserializer} /
 * {@code KafkaAvroSerializer} and share the same Schema Registry endpoint
 * ({@code platform.schema-registry.url}, 2.1).
 *
 * <p>The Flink production path uses
 * {@link com.java.processing.operator.ShoppingEventDeserializationSchema} and
 * {@link com.java.processing.operator.ShoppingEventSerializationSchema} which hold
 * transient Confluent serde instances initialised in {@code open()}.
 *
 * <p>Security (SASL_SSL / mTLS) comes from {@code spring.kafka.properties.*}
 * via {@link KafkaProperties} (OWASP A02).
 */
@Configuration
@RequiredArgsConstructor
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    /** Confluent Schema Registry URL — required by KafkaAvroSerializer/Deserializer. */
    @Value("${platform.schema-registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    // ---- Consumer (raw topic) -----------------------------------------------

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

    // ---- Producer (aggregates topic) ----------------------------------------

    @Bean
    public ProducerFactory<String, ShoppingEvent> producerFactory() {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ShoppingEventConfluentSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, ShoppingEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
