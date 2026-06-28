package com.java.ingestion.dlq;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Micrometer counters for DLQ replay operations (OB-4).
 *
 * <p>Exposed metrics:
 * <ul>
 *   <li>{@code ads_dlq_replayed_total} — events successfully re-published to raw topic.</li>
 *   <li>{@code ads_dlq_replay_failed_total} — events that failed re-publish after retries.</li>
 * </ul>
 *
 * <p>Grafana panel: "DLQ Replay Activity" in {@code ad-analytics-overview.json}.
 * Alert: {@code DlqLagGrowing} in {@code alert-rules.yml} fires when consumer group
 * lag on {@code *.ad-interaction-failed-*} topics exceeds 10 000 for 15 minutes.
 */
@Component
public final class DlqReplayMetrics {

    private final Counter replayed;
    private final Counter failed;

    public DlqReplayMetrics(MeterRegistry registry) {
        this.replayed = Counter.builder("ads_dlq_replayed_total")
                .description("Events successfully re-published from DLQ to raw topic")
                .register(registry);
        this.failed = Counter.builder("ads_dlq_replay_failed_total")
                .description("DLQ events that failed re-publish even after retries")
                .register(registry);
    }

    public void recordReplayed(int count)  { replayed.increment(count); }
    public void recordFailed(int count)    { failed.increment(count);   }
}

