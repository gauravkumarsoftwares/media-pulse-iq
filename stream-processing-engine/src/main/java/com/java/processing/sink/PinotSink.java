package com.java.processing.sink;

import com.java.model.ShoppingEvent;

/**
 * Abstraction over the real-time serving store (architecture §4).
 *
 * <p>In production this writes enriched/aggregated rows to Apache Pinot (and hot
 * counters to Redis). The default implementation publishes to the aggregates
 * Kafka topic, decoupling the stream engine from the serving layer so either
 * side can scale and evolve independently (Dependency Inversion).
 */
public interface PinotSink {

    /**
     * Upsert an enriched event into the serving layer.
     *
     * @param event the enriched or synthesized event to persist for querying
     */
    void upsert(ShoppingEvent event);
}

