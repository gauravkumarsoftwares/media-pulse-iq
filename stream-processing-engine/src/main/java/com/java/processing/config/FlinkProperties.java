package com.java.processing.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Externalised configuration for the embedded Apache Flink job (architecture §3).
 *
 * <p>Bound from {@code platform.flink.*} in application[-profile].yml.
 * All values have safe defaults that work in a local mini-cluster environment;
 * production overrides are supplied via K8s ConfigMap / Secrets.
 */
@Component
@ConfigurationProperties(prefix = "platform.flink")
@Getter
@Setter
public class FlinkProperties {

    /**
     * Set to {@code true} to start the Flink DataStream job instead of the
     * Spring-Kafka fallback consumer. Defaults to {@code false} so that the
     * service can boot locally without a Flink runtime.
     */
    private boolean enabled = false;

    /**
     * Default operator parallelism. For production Flink clusters, align this
     * with the number of Kafka partitions (architecture §2.2).
     */
    private int parallelism = 1;

    /**
     * Flink checkpointing interval in milliseconds.
     * Set to 0 to disable checkpointing (local/dev only).
     */
    private long checkpointIntervalMs = 30_000L;

    /**
     * FileSystem URI for checkpoint storage, e.g.
     * {@code s3://my-bucket/flink-checkpoints/stream-engine}.
     * Leave blank to use the default job-manager memory store (dev only).
     */
    private String checkpointDir = "";

    /**
     * Use RocksDB incremental state backend for production workloads.
     * Falls back to the default in-memory backend when {@code false}.
     */
    private boolean useRocksDb = false;

    /**
     * Kafka bootstrap servers (may be overridden independently of
     * {@code spring.kafka.bootstrap-servers} for the Flink source/sink).
     * If blank, falls back to {@code spring.kafka.bootstrap-servers}.
     */
    private String kafkaBootstrapServers = "";

    /** Redis host for the hot-counter sink. */
    private String redisHost = "localhost";

    /** Redis port for the hot-counter sink. */
    private int redisPort = 6379;

    /** Redis password (blank = no auth). */
    private String redisPassword = "";

    /** Redis database index (0-15). */
    private int redisDatabase = 0;

    /** TTL (seconds) applied to Redis campaign-counter hash keys. */
    private long redisTtlSeconds = 172_800L;   // 48 hours

    /**
     * Confluent Schema Registry URL used by the Flink
     * {@link com.java.processing.operator.ShoppingEventDeserializationSchema} and
     * {@link com.java.processing.operator.ShoppingEventSerializationSchema} to
     * initialise their transient {@code KafkaAvroDeserializer} / {@code KafkaAvroSerializer}
     * instances (architecture §2.1).
     *
     * <p>Defaults to local Confluent Platform / Docker Compose endpoint.
     * Override to e.g. {@code https://psrc-xxxxx.us-east-2.aws.confluent.cloud} for
     * Confluent Cloud.
     */
    private String schemaRegistryUrl = "http://localhost:8081";
}

