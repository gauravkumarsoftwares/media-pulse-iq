package com.java.processing.operator;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventAvroSerde;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink deserialization schema that converts Confluent Avro wire-format Kafka records
 * into {@link ShoppingEvent} POJOs (architecture §2.1, §3).
 *
 * <p>Wire format consumed: {@code [0x00][4-byte schemaId][Avro binary]}.
 *
 * <p>The Confluent {@link KafkaAvroDeserializer} is held as a {@code transient} field
 * (not serialisable) and initialised in {@link #open} — the same pattern used by
 * {@link com.java.processing.sink.RedisHotCounterSink} for its {@code JedisPool}.
 * Flink calls {@code open()} on every TaskManager before processing begins.
 *
 * <p>The resulting {@link GenericRecord} is mapped to the {@link ShoppingEvent} POJO
 * via {@link ShoppingEventAvroSerde#fromRecord}.
 */
public final class ShoppingEventDeserializationSchema
        implements KafkaRecordDeserializationSchema<ShoppingEvent> {

    private static final long serialVersionUID = 3L;

    /** Schema Registry URL — serialised so Flink can restore it after checkpoint recovery. */
    private final String schemaRegistryUrl;

    /** Confluent deserialiser — transient, created in {@link #open}. */
    private transient KafkaAvroDeserializer avroDeserializer;

    public ShoppingEventDeserializationSchema(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        // Return GenericRecord (not SpecificRecord) — mapping to POJO is done by ShoppingEventAvroSerde.
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        avroDeserializer = new KafkaAvroDeserializer();
        avroDeserializer.configure(config, false);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record,
                            Collector<ShoppingEvent> out) throws IOException {
        if (record.value() == null || record.value().length == 0) {
            return;
        }
        GenericRecord avroRecord = (GenericRecord) avroDeserializer.deserialize(
                record.topic(), record.value());
        if (avroRecord != null) {
            out.collect(ShoppingEventAvroSerde.fromRecord(avroRecord));
        }
    }

    @Override
    public TypeInformation<ShoppingEvent> getProducedType() {
        return TypeInformation.of(ShoppingEvent.class);
    }
}
