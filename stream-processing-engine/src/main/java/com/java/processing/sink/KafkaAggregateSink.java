package com.java.processing.sink;

import com.java.model.ShoppingEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Default {@link PinotSink} that publishes enriched events to the DDD-named enriched topic.
 * Both Pinot RealtimeTable and insights-query-service AggregateConsumer read from it.
 *
 * <p><strong>Fallback mode only.</strong> Active when platform.flink.enabled=false.
 */
@Component
@ConditionalOnProperty(name = "platform.flink.enabled", havingValue = "false", matchIfMissing = true)
@RequiredArgsConstructor
public final class KafkaAggregateSink implements PinotSink {

    private final KafkaTemplate<String, ShoppingEvent> kafkaTemplate;

    @Value("${platform.kafka.topic.enriched}")
    private String topicEnriched;

    @Override
    public void upsert(ShoppingEvent event) {
        String key = event.getTenantId() + ":" + event.getCampaignId();
        kafkaTemplate.send(topicEnriched, key, event);
    }
}
