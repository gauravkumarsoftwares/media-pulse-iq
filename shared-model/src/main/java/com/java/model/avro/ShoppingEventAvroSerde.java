package com.java.model.avro;

import com.java.model.ShoppingEvent;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.compiler.idl.Idl;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * POJO ↔ Avro {@link GenericRecord} mapping utilities for {@link ShoppingEvent}
 * (architecture 2.1).
 *
 * <p>The Avro schema is defined in
 * {@code shared-model/src/main/resources/avro/shopping_event.avdl} (Avro IDL format)
 * and parsed once at class initialisation via the {@code avro-compiler} {@link Idl}
 * parser.
 *
 * <p><strong>Wire format is handled entirely by the Confluent serde layer:</strong>
 * <ul>
 *   <li>Spring Kafka producers/consumers: {@link ShoppingEventConfluentSerializer} /
 *       {@link ShoppingEventConfluentDeserializer} (delegates to
 *       {@code io.confluent.kafka.serializers.KafkaAvroSerializer} /
 *       {@code KafkaAvroDeserializer}).
 *   <li>Flink {@code KafkaSource} / {@code KafkaSink}: the Flink serialisation schemas
 *       hold {@code transient KafkaAvroSerializer} / {@code KafkaAvroDeserializer} fields
 *       initialised in {@code open()}.
 * </ul>
 *
 * <p>Both paths produce / consume the Confluent wire format:
 * {@code [0x00][4-byte schemaId][Avro binary]}.  Schema registration is automatic
 * ({@code auto.register.schemas=true} by default) using {@code TopicNameStrategy}
 * ({@code {topic}-value}).
 *
 * <p>This class is thread-safe; the {@link Schema} is immutable and shared.
 */
public final class ShoppingEventAvroSerde {

    /**
     * Avro schema parsed once from {@code avro/shopping_event.avdl} on the classpath.
     * Shared across all threads / serde instances.
     */
    public static final Schema SCHEMA;

    static {
        try (InputStream is = ShoppingEventAvroSerde.class
                    .getClassLoader()
                    .getResourceAsStream("avro/shopping_event.avdl")) {
            if (is == null) {
                throw new IllegalStateException(
                    "Avro IDL not found on classpath: avro/shopping_event.avdl. "
                    + "Ensure shared-model.jar is on the runtime classpath.");
            }
            // Idl parses the .avdl protocol file → Protocol → extract the record schema.
            try (Idl idl = new Idl(is)) {
                Protocol protocol = idl.CompilationUnit();
                SCHEMA = protocol.getType("com.java.model.ShoppingEvent");
                if (SCHEMA == null) {
                    throw new IllegalStateException(
                        "Record 'com.java.model.ShoppingEvent' not found in shopping_event.avdl protocol.");
                }
            }
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private ShoppingEventAvroSerde() {
        // utility class
    }

    // ---- Public API (POJO ↔ GenericRecord) ---------------------------------

    /**
     * Map a {@link ShoppingEvent} POJO to an Avro {@link GenericRecord} suitable
     * for passing to {@code KafkaAvroSerializer.serialize(topic, record)}.
     *
     * @param event the event to convert; must not be {@code null}
     */
    public static GenericRecord toRecord(ShoppingEvent event) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("eventId",           event.getEventId());
        record.put("tenantId",          event.getTenantId());
        record.put("userId",            event.getUserId());
        record.put("sessionId",         event.getSessionId());
        record.put("campaignId",        event.getCampaignId());
        record.put("eventType",         event.getEventType());
        record.put("eventTimestampMs",  event.getEventTimestampMs());
        record.put("cost",              event.getCost());
        // Avro map field must not be null; substitute an empty map for null customTags.
        record.put("customTags",
                event.getCustomTags() != null ? event.getCustomTags() : Collections.emptyMap());
        return record;
    }

    /**
     * Map an Avro {@link GenericRecord} (returned by
     * {@code KafkaAvroDeserializer.deserialize(topic, bytes)}) back to a
     * {@link ShoppingEvent} POJO.
     *
     * <p>Avro returns {@link org.apache.avro.util.Utf8} instances for {@code string}
     * fields; {@link #str(GenericRecord, String)} calls {@code .toString()} to convert.
     *
     * @param record the Avro record; must not be {@code null}
     */
    @SuppressWarnings("unchecked")
    public static ShoppingEvent fromRecord(GenericRecord record) {
        Map<?, ?> rawTags = (Map<?, ?>) record.get("customTags");
        Map<String, String> tags = null;
        if (rawTags != null && !rawTags.isEmpty()) {
            Map<String, String> mutable = new HashMap<>(rawTags.size());
            rawTags.forEach((k, v) -> mutable.put(k.toString(), v == null ? null : v.toString()));
            tags = mutable;
        }

        return ShoppingEvent.builder()
                .eventId(        str(record, "eventId"))
                .tenantId(       str(record, "tenantId"))
                .userId(         str(record, "userId"))
                .sessionId(      str(record, "sessionId"))
                .campaignId(     str(record, "campaignId"))
                .eventType(      str(record, "eventType"))
                .eventTimestampMs((Long)   record.get("eventTimestampMs"))
                .cost(            (Double) record.get("cost"))
                .customTags(tags)
                .build();
    }

    /** Null-safe field extraction that handles Avro's {@code Utf8} return type. */
    private static String str(GenericRecord record, String field) {
        Object val = record.get(field);
        return val == null ? null : val.toString();
    }
}
