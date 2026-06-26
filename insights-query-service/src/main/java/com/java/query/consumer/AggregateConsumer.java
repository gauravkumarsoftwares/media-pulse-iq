package com.java.query.consumer;

import com.java.model.ShoppingEvent;
import com.java.query.store.StarTreeStore;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes enriched and attributed events from the DDD-named enriched topic and
 * indexes them into the local OLAP store that backs the read APIs.
 *
 * Topic: ${platform.kafka.topic.enriched}
 * (e.g. prod.internal.event.ads.attribution.ad-interaction-enriched-by-campaign)
 */
@Component
@RequiredArgsConstructor
public final class AggregateConsumer {

    private final StarTreeStore store;

    @KafkaListener(topics = "${platform.kafka.topic.enriched}",
            groupId = "${platform.kafka.consumer-group.insights-serving}",
            containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(ShoppingEvent event) {
        store.index(event);
    }
}
