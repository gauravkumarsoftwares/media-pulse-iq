package com.java.ingestion.producer;

import com.java.ingestion.observability.IngestionMetrics;
import com.java.model.ShoppingEvent;
import com.java.security.pii.PiiMasker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Routes rejected/invalid events to the Dead Letter Queue (architecture section 2.2, 8.3).
 *
 * <p>Topic: {@code ${platform.kafka.topic.dlq}}
 * (resolved at runtime to e.g.
 * {@code prod.internal.event.ads.clickstream.ad-interaction-failed-by-tenant-id}).
 *
 * <p>The payload is annotated with a {@code dlq-reason} Kafka header so SRE tooling
 * can triage and replay it later. PII is redacted before publishing (OWASP A04/A09).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public final class DlqProducer {

    private final KafkaTemplate<String, ShoppingEvent> kafkaTemplate;

    private final IngestionMetrics metrics;

    /**
     * Resolved at startup from the DDD-named topic property.
     */
    @Value("${platform.kafka.topic.dlq}")
    private String topicDlq;

    /**
     * Publish a rejected event to the DLQ with the failure reason attached.
     *
     * @param rawEvent the original (possibly incomplete) payload
     * @param reason   human-readable rejection reason / error code
     */
    public void sendToDlq(ShoppingEvent rawEvent, String reason) {
        ShoppingEvent redacted = redact(rawEvent);
        String key = redacted.getTenantId() != null ? redacted.getTenantId() : "unknown";
        ProducerRecord<String, ShoppingEvent> record = new ProducerRecord<>(topicDlq, key, redacted);
        record.headers().add(new RecordHeader("dlq-reason", reason.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record).whenComplete((metadata, ex) -> {
            if (ex != null) {
                log.error("[PRODUCER] Permanent send failure — routing event {} to DLQ: {}",
                        redacted.getEventId(), PiiMasker.mask(reason), ex);
                metrics.recordDlq(rawEvent.getTenantId(), reason);
            }
        });
        log.warn("Routed event {} to DLQ: {}", redacted.getEventId(), PiiMasker.mask(reason));
    }

    /**
     * Return a PII-minimized copy safe for the DLQ.
     */
    private ShoppingEvent redact(ShoppingEvent in) {
        ShoppingEvent out = new ShoppingEvent();
        out.setEventId(in.getEventId());
        out.setTenantId(in.getTenantId());
        out.setUserId(PiiMasker.pseudonymize(in.getUserId()));
        out.setSessionId(in.getSessionId());
        out.setCampaignId(in.getCampaignId());
        out.setEventType(in.getEventType());
        out.setEventTimestampMs(in.getEventTimestampMs());
        out.setCost(in.getCost());
        // customTags intentionally dropped - may carry tenant-specific PII.
        return out;
    }
}
