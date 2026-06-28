package com.java.query.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Externalised query-engine tuning knobs (BUCKET-CAP fix).
 *
 * <p>Bound from {@code platform.query.*} in application[-profile].yml.
 */
@Component
@ConfigurationProperties(prefix = "platform.query")
@Getter
@Setter
public class QueryProperties {

    /**
     * Maximum number of grain-aligned time buckets that
     * {@link com.java.query.service.TimeSeriesBucketUtils} will produce.
     * Prevents accidental OOM on very long windows with fine-grained buckets.
     * Default: 10 000 (covers 416+ days at hourly grain).
     */
    private int maxBuckets = 10_000;
}

