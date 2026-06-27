package com.java.model.avro;

import com.java.model.ShoppingEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Kafka {@link Serializer} for {@link ShoppingEvent} using the Confluent Avro wire format
 * (architecture 2.1).
 *
 * <p>Wire format produced: {@code [0x00][4-byte schemaId][Avro binary]}.
 *
 * <p>This serialiser delegates to Confluent's {@link KafkaAvroSerializer} for the actual
 * binary encoding and Schema Registry interaction.  The conversion from the {@link ShoppingEvent}
 * POJO to an Avro {@link GenericRecord} is handled by {@link ShoppingEventAvroSerde#toRecord}.
 *
 * <p><strong>Required configuration</strong> (pass via producer properties or
 * {@code spring.kafka.properties.*}):
 * <pre>
 *   schema.registry.url = http://localhost:8081   # or Confluent Cloud SR URL
 *   auto.register.schemas = true                  # default; set false in prod if schemas are pre-registered
 * </pre>
 *
 * <p><strong>Register on a Kafka producer via:</strong>
 * <pre>
 *   props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
 *             ShoppingEventConfluentSerializer.class);
 *   props.put("schema.registry.url", schemaRegistryUrl);
 * </pre>
 *
 * <p>Schema subject: {@code {topic}-value} (TopicNameStrategy — Confluent default).
 * The schema registered is the {@link ShoppingEventAvroSerde#SCHEMA} derived from
 * {@code avro/shopping_event.avdl}.
 */
public final class ShoppingEventConfluentSerializer implements Serializer<ShoppingEvent> {

    /**
     * Inner Confluent serialiser.  Created in {@link #configure}; lifecycle bound to
     * this serialiser's lifecycle.
     */
    private KafkaAvroSerializer inner;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner = new KafkaAvroSerializer();
        inner.configure(configs, isKey);
    }

    /**
     * Converts the POJO to a {@link GenericRecord} and delegates encoding +
     * Schema Registry registration to the Confluent serialiser.
     *
     * @return Confluent Avro wire-format bytes, or {@code null} if {@code data} is null
     */
    @Override
    public byte[] serialize(String topic, ShoppingEvent data) {
        if (data == null) {
            return null;
        }
        GenericRecord record = ShoppingEventAvroSerde.toRecord(data);
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }
}

