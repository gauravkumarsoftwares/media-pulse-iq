package com.java.ingestion.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Centralised Micrometer metrics for the ingestion write-path (Phase 6 Observability).
 *
 * <h2>Metrics emitted</h2>
 * <pre>
 *  ads.events.ingested{tenantId,eventType,status}  – Counter  – events accepted / rejected
 *  ads.validation.failures{tenantId,reason}         – Counter  – schema validation failures
 *  ads.events.dlq{tenantId,reason}                  – Counter  – events routed to DLQ
 *  ads.ingest.latency{tenantId,status}              – Timer    – end-to-end request latency
 * </pre>
 *
 * <p><strong>Cardinality note:</strong> {@code tenantId} is included as a tag so SRE teams
 * can identify misbehaving tenants. In deployments with thousands of tenants, replace with
 * a bucketed tier label ({@code enterprise / standard / free}) to avoid metric explosion.
 */
@Component
@RequiredArgsConstructor
public class IngestionMetrics {

    private final MeterRegistry registry;

    // ---- metric name constants -------------------------------------------
    public static final String EVENTS_INGESTED     = "ads.events.ingested";
    public static final String VALIDATION_FAILURES = "ads.validation.failures";
    public static final String EVENTS_DLQ          = "ads.events.dlq";
    public static final String INGEST_LATENCY      = "ads.ingest.latency";

    // ---- public API --------------------------------------------------------

    /** Record a successfully accepted and published event. */
    public void recordAccepted(String tenantId, String eventType) {
        counter(EVENTS_INGESTED, "tenantId", safe(tenantId),
                "eventType", safe(eventType), "status", "accepted").increment();
    }

    /** Record a validation failure that was routed to the DLQ. */
    public void recordValidationFailure(String tenantId, String reason) {
        counter(EVENTS_INGESTED, "tenantId", safe(tenantId),
                "eventType", "unknown", "status", "validation_failed").increment();
        counter(VALIDATION_FAILURES, "tenantId", safe(tenantId),
                "reason", abbreviate(reason)).increment();
    }

    /** Record any DLQ routing (validation failure OR internal error). */
    public void recordDlq(String tenantId, String reason) {
        counter(EVENTS_DLQ, "tenantId", safe(tenantId),
                "reason", abbreviate(reason)).increment();
    }

    /** Start a latency sample — call {@link #stopTimer} in a {@code finally} block. */
    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    /**
     * Stop a latency sample and record it.
     *
     * @param sample    the sample returned by {@link #startTimer()}
     * @param tenantId  tenant scope
     * @param status    outcome label: {@code accepted}, {@code validation_failed}, {@code error}
     */
    public void stopTimer(Timer.Sample sample, String tenantId, String status) {
        sample.stop(Timer.builder(INGEST_LATENCY)
                .description("End-to-end HTTP ingest latency")
                .tag("tenantId", safe(tenantId))
                .tag("status", status)
                .register(registry));
    }

    // ---- helpers -----------------------------------------------------------

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }

    /** Null-safe tenant label. */
    private static String safe(String value) {
        return (value == null || value.isBlank()) ? "unknown" : value;
    }

    /** Cap reason string to 80 chars to keep tag values prometheus-friendly. */
    private static String abbreviate(String s) {
        if (s == null) return "unknown";
        return s.length() > 80 ? s.substring(0, 77) + "..." : s;
    }
}

