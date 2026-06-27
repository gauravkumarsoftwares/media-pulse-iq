package com.java.query.store;

import com.java.model.ShoppingEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * In-memory, multi-dimensional aggregation store standing in for Apache Pinot's
 * Star-Tree index (architecture 4). Counts are keyed by
 * (tenant_id, campaign_id, event_type) and incremented with lock-free adders
 * for O(1) writes and reads.
 *
 * <p>In production this is replaced by a Pinot client; the query contract
 * ({@link #count}) remains identical.
 */
@Component
public final class StarTreeStore {

    private final ConcurrentHashMap<Key, LongAdder> buckets = new ConcurrentHashMap<>();

    /** Index one enriched event into the aggregation store. */
    public void index(ShoppingEvent event) {
        if (event == null || event.getCampaignId() == null) {
            return;
        }
        Key key = new Key(event.getTenantId(), event.getCampaignId(), event.getEventType());
        buckets.computeIfAbsent(key, k -> new LongAdder()).increment();
    }

    /** @return the aggregated count for the given dimensions (0 if absent). */
    public long count(String tenantId, String campaignId, String eventType) {
        LongAdder adder = buckets.get(new Key(tenantId, campaignId, eventType));
        return adder == null ? 0L : adder.sum();
    }

    /** Immutable composite dimension key. */
    private record Key(String tenantId, String campaignId, String eventType) { }
}

