package com.java.model.avro;

import com.java.model.ShoppingEvent;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka {@link Deserializer} for {@link ShoppingEvent} using the Confluent Avro wire format
 * (architecture §2.1).
 *
 * <p>Wire format consumed: {@code [0x00][4-byte schemaId][Avro binary]}.
 *
 * <p>This deserialiser delegates to Confluent's {@link KafkaAvroDeserializer} for the
 * binary decoding and Schema Registry lookup, then converts the resulting
 * {@link GenericRecord} to a {@link ShoppingEvent} POJO via
 * {@link ShoppingEventAvroSerde#fromRecord}.
 *
 * <p>{@link KafkaAvroDeserializerConfig#SPECIFIC_AVRO_READER_CONFIG} is always forced to
 * {@code false} so the inner deserialiser returns a {@link GenericRecord} that this class
 * can safely cast and map — regardless of any caller-provided configuration.
 *
 * <p><strong>Required configuration</strong> (pass via consumer properties or
 * {@code spring.kafka.properties.*}):
 * <pre>
 *   schema.registry.url = http://localhost:8081
 * </pre>
 *
 * <p><strong>Register on a Kafka consumer via:</strong>
 * <pre>
 *   new DefaultKafkaConsumerFactory&lt;&gt;(props,
 *       new StringDeserializer(),
 *       new ShoppingEventConfluentDeserializer());
 * </pre>
 */
public final class ShoppingEventConfluentDeserializer implements Deserializer<ShoppingEvent> {

    /**
     * Inner Confluent deserialiser.  Created in {@link #configure}; lifecycle bound to
     * this deserialiser's lifecycle.
     */
    private KafkaAvroDeserializer inner;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Force GenericRecord mode so we control the POJO mapping ourselves.
        Map<String, Object> merged = new HashMap<>(configs);
        merged.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        inner = new KafkaAvroDeserializer();
        inner.configure(merged, isKey);
    }

    /**
     * Decodes the Confluent wire-format bytes via the inner Confluent deserialiser
     * and maps the resulting {@link GenericRecord} to a {@link ShoppingEvent} POJO.
     *
     * @return the deserialised event, or {@code null} for a null / empty payload
     */
    @Override
    public ShoppingEvent deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        GenericRecord record = (GenericRecord) inner.deserialize(topic, data);
        return record == null ? null : ShoppingEventAvroSerde.fromRecord(record);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }
}

