package com.java.processing.operator;

import com.java.model.ShoppingEvent;
import com.java.model.avro.ShoppingEventAvroSerde;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink serialisation schema that converts a {@link ShoppingEvent} POJO to a Confluent
 * Avro wire-format Kafka {@link ProducerRecord} (architecture 2.1, 3).
 *
 * <p>Wire format produced: {@code [0x00][4-byte schemaId][Avro binary]}.
 *
 * <p>The Confluent {@link KafkaAvroSerializer} is held as a {@code transient} field
 * (not serialisable) and initialised in {@link #open} — the same pattern used by
 * {@link com.java.processing.sink.RedisHotCounterSink} for its {@code JedisPool}.
 * Flink calls {@code open()} on every TaskManager before processing begins.
 *
 * <p>The {@link ShoppingEvent} POJO is first converted to an Avro {@link GenericRecord}
 * via {@link ShoppingEventAvroSerde#toRecord}, which is then serialised by the Confluent
 * serialiser. The schema is auto-registered in Schema Registry under
 * {@code {topic}-value} (TopicNameStrategy).
 *
 * <p>Record key: {@code "{tenantId}:{campaignId}"} — ensures campaign aggregates
 * co-locate on the same Kafka partition for Pinot's consumption (architecture 2.3).
 */
public final class ShoppingEventSerializationSchema
        implements KafkaRecordSerializationSchema<ShoppingEvent> {

    private static final long serialVersionUID = 3L;

    private final String topic;

    /** Schema Registry URL — serialised so Flink can restore it after checkpoint recovery. */
    private final String schemaRegistryUrl;

    /** Confluent serialiser — transient, created in {@link #open}. */
    private transient KafkaAvroSerializer avroSerializer;

    public ShoppingEventSerializationSchema(String topic, String schemaRegistryUrl) {
        this.topic             = topic;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context,
                     KafkaSinkContext sinkContext) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        // auto.register.schemas = true by default; set to false in prod if schemas are pre-registered.
        avroSerializer = new KafkaAvroSerializer();
        avroSerializer.configure(config, false);
    }

    @Override
    @Nullable
    public ProducerRecord<byte[], byte[]> serialize(ShoppingEvent element,
                                                     KafkaSinkContext context,
                                                     Long timestamp) {
        byte[] key = (element.getTenantId() + ":" + element.getCampaignId())
                .getBytes(StandardCharsets.UTF_8);
        GenericRecord record = ShoppingEventAvroSerde.toRecord(element);
        byte[] value = avroSerializer.serialize(topic, record);
        return new ProducerRecord<>(topic, key, value);
    }
}
