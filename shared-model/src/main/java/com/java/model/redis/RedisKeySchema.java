package com.java.model.redis;

/**
 * Single source of truth for all Redis key and field name patterns used across
 * the platform (stream-processing-engine writes, insights-query-service reads).
 *
 * <h3>Key schemas</h3>
 * <pre>
 *   Hash key  :  campaign:{tenantId}:{campaignId}
 *   Hash field:  {eventType}   (CLICK, IMPRESSION, CLICK_TO_BASKET)
 *   Value     :  counter (HINCRBY — total aggregate)
 *
 *   TS hash key  :  ts:{tenantId}:{campaignId}:{eventType}
 *   TS hash field:  {hourBucket_epoch_ms}  (hour-aligned, milliseconds)
 *   TS value     :  counter per bucket (HINCRBY)
 * </pre>
 *
 * <p>Placing key-building logic here prevents the key format from drifting
 * independently in the Flink sink and the Spring query service (DRY / D3).
 */
public final class RedisKeySchema {

    private static final String HASH_PREFIX       = "campaign";
    private static final String TIMESERIES_PREFIX = "ts";
    private static final long   HOUR_MS           = 3_600_000L;

    private RedisKeySchema() {}

    /**
     * Aggregate hash key for a campaign: {@code campaign:{tenantId}:{campaignId}}.
     * Hash field = event type; value = total count (HINCRBY).
     */
    public static String hashKey(String tenantId, String campaignId) {
        return HASH_PREFIX + ":" + tenantId + ":" + campaignId;
    }

    /**
     * Time-series hash key for per-hour bucket counts:
     * {@code ts:{tenantId}:{campaignId}:{eventType}}.
     * Hash field = hour-bucket epoch ms (see {@link #hourBucket});
     * value = count for that hour (HINCRBY).
     */
    public static String timeSeriesKey(String tenantId, String campaignId, String eventType) {
        return TIMESERIES_PREFIX + ":" + tenantId + ":" + campaignId + ":" + eventType;
    }

    /**
     * Truncate an event timestamp to the start of its UTC hour boundary (milliseconds).
     * Used as the hash field in the time-series key so all events in the same hour
     * share the same field and their counts accumulate correctly.
     *
     * @param eventTimestampMs raw event timestamp in epoch milliseconds
     * @return epoch ms of the hour-aligned bucket start
     */
    public static long hourBucket(long eventTimestampMs) {
        return (eventTimestampMs / HOUR_MS) * HOUR_MS;
    }
}

