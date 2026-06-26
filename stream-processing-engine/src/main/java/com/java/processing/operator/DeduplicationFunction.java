package com.java.processing.operator;

import com.java.model.ShoppingEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Flink idempotent deduplication operator (architecture §3.1).
 *
 * <p>Keyed by {@code eventId}. For each event, it checks whether an identical ID
 * has been processed within the configured TTL window; if so it is silently
 * dropped, otherwise it is forwarded downstream and the ID is recorded in Flink
 * managed {@link ValueState} with automatic TTL-based expiry.
 *
 * <p>In production the state backend is RocksDB (incremental checkpoints), so
 * the dedup window can span billions of events without exhausting heap.
 */
public final class DeduplicationFunction
        extends KeyedProcessFunction<String, ShoppingEvent, ShoppingEvent> {

    private static final long serialVersionUID = 1L;

    private final long ttlMinutes;

    /** Keyed state: stores the event timestamp when the ID was first seen. */
    private transient ValueState<Long> seenState;

    public DeduplicationFunction(long ttlMinutes) {
        this.ttlMinutes = ttlMinutes;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(ttlMinutes))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1_000)  // amortised cleanup during compaction
                .build();

        ValueStateDescriptor<Long> descriptor =
                new ValueStateDescriptor<>("dedup-seen-ts", Types.LONG);
        descriptor.enableTimeToLive(ttlConfig);

        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(ShoppingEvent event,
                               Context ctx,
                               Collector<ShoppingEvent> out) throws Exception {
        if (seenState.value() == null) {
            // First time we see this eventId within the TTL window — record and forward.
            seenState.update(event.getEventTimestampMs());
            out.collect(event);
        }
        // Duplicate: state already present → drop silently.
    }
}

