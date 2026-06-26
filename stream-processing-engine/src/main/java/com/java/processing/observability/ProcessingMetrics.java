package com.java.processing.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Micrometer metrics for the stream-processing-engine (Phase 6 Observability).
 *
 * <h2>Metrics emitted</h2>
 * <pre>
 *  ads.events.processed{tenantId,eventType}     – Counter  – unique events forwarded to serving layer
 *  ads.events.dedup.dropped{tenantId}           – Counter  – duplicates suppressed by dedup window
 *  ads.events.attributed{tenantId,campaignId}   – Counter  – CLICK_TO_BASKET conversions emitted
 *  ads.processing.latency{tenantId,eventType}   – Timer    – per-event end-to-end processing time
 *  ads.dedup.state.size                         – Gauge    – current dedup map cardinality (local)
 * </pre>
 *
 * <p>When Flink is enabled ({@code platform.flink.enabled=true}) these metrics are recorded
 * from Spring-managed beans (not Flink TaskManagers). For native Flink metrics, configure
 * the Flink Prometheus reporter via {@code flink-conf.yaml}.
 */
@Component
@RequiredArgsConstructor
public class ProcessingMetrics {

    private final MeterRegistry registry;

    // Gauge backing value for the dedup state size
    private final AtomicLong dedupStateSize = new AtomicLong(0);

    public static final String EVENTS_PROCESSED  = "ads.events.processed";
    public static final String DEDUP_DROPPED     = "ads.events.dedup.dropped";
    public static final String ATTRIBUTED        = "ads.events.attributed";
    public static final String PROCESSING_LATENCY = "ads.processing.latency";
    public static final String DEDUP_STATE_SIZE  = "ads.dedup.state.size";

    // ---- public API --------------------------------------------------------

    /** Record a unique event forwarded to the enriched topic / serving layer. */
    public void recordProcessed(String tenantId, String eventType) {
        counter(EVENTS_PROCESSED,
                "tenantId", safe(tenantId),
                "eventType", safe(eventType))
                .increment();
    }

    /** Record a duplicate event that was dropped by the deduplication window. */
    public void recordDedupDropped(String tenantId) {
        counter(DEDUP_DROPPED, "tenantId", safe(tenantId)).increment();
    }

    /**
     * Record a successful CLICK_TO_BASKET attribution.
     *
     * @param tenantId   tenant scope
     * @param campaignId campaign that received the attribution
     */
    public void recordAttributed(String tenantId, String campaignId) {
        counter(ATTRIBUTED,
                "tenantId", safe(tenantId),
                "campaignId", safe(campaignId))
                .increment();
    }

    /** Start a per-event processing latency sample. */
    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    /** Stop and record a processing latency sample. */
    public void stopTimer(Timer.Sample sample, String tenantId, String eventType) {
        sample.stop(Timer.builder(PROCESSING_LATENCY)
                .description("Per-event stream processing latency")
                .tag("tenantId", safe(tenantId))
                .tag("eventType", safe(eventType))
                .register(registry));
    }

    /**
     * Publish the current dedup state size as a Gauge (local in-memory dedup only).
     * Call from {@link com.java.processing.cleaner.Deduplicator#size()}.
     */
    public void setDedupStateSize(long size) {
        dedupStateSize.set(size);
        // Register gauge lazily (idempotent after first registration)
        Gauge.builder(DEDUP_STATE_SIZE, dedupStateSize, AtomicLong::doubleValue)
                .description("Current number of event IDs tracked by the dedup filter")
                .register(registry);
    }

    // ---- helpers -----------------------------------------------------------

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }

    private static String safe(String v) {
        return (v == null || v.isBlank()) ? "unknown" : v;
    }
}

