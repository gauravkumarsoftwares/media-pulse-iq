package com.java.processing.sink;

import com.java.model.ShoppingEvent;
import com.java.model.redis.RedisKeySchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Flink sink that writes campaign hot-counters and per-hour time-series buckets
 * to Redis (architecture 4, hot tier &lt;48 h).
 *
 * <h3>Keys written per event</h3>
 * <pre>
 *   HINCRBY campaign:{tenantId}:{campaignId}  {eventType}     1   — aggregate counter
 *   HINCRBY ts:{tenantId}:{campaignId}:{eventType}  {hourBucket_ms}  1  — time-series bucket
 * </pre>
 *
 * <p>Key formats are defined in {@link RedisKeySchema} (shared-model) so this
 * sink and {@link com.java.query.store.RedisInsightsStore} always use the same
 * schema (DRY — D3). The time-series hash enables
 * {@link com.java.query.handler.RedisCacheTierHandler} to serve exact
 * per-hour counts (REDIS-TS fix) instead of even-distribution approximations.
 *
 * <p>Both keys share the same TTL (refreshed on every write).
 */
@Slf4j
public final class RedisHotCounterSink extends RichSinkFunction<ShoppingEvent> {

    private static final long serialVersionUID = 1L;

    private final String redisHost;
    private final int    redisPort;
    private final String redisPassword;
    private final int    redisDatabase;
    private final long   ttlSeconds;

    /** Created in open(), closed in close() — never serialised. */
    private transient JedisPool jedisPool;

    public RedisHotCounterSink(String redisHost, int redisPort,
                                String redisPassword, int redisDatabase,
                                long ttlSeconds) {
        this.redisHost     = redisHost;
        this.redisPort     = redisPort;
        this.redisPassword = redisPassword;
        this.redisDatabase = redisDatabase;
        this.ttlSeconds    = ttlSeconds;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(8);
        poolConfig.setMaxIdle(4);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);

        String pwd = (redisPassword == null || redisPassword.isBlank()) ? null : redisPassword;
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2_000, pwd, redisDatabase);
        log.info("RedisHotCounterSink connected to {}:{} db={}", redisHost, redisPort, redisDatabase);
    }

    @Override
    public void invoke(ShoppingEvent event, Context context) {
        if (event.getTenantId() == null || event.getCampaignId() == null
                || event.getEventType() == null) {
            return;
        }

        String hashKey = RedisKeySchema.hashKey(event.getTenantId(), event.getCampaignId());
        String tsKey   = RedisKeySchema.timeSeriesKey(
                event.getTenantId(), event.getCampaignId(), event.getEventType());
        long   bucket  = RedisKeySchema.hourBucket(event.getEventTimestampMs());

        try (Jedis jedis = jedisPool.getResource()) {
            // Aggregate counter (HGET used by resolveCount)
            jedis.hincrBy(hashKey, event.getEventType(), 1L);
            jedis.expire(hashKey, ttlSeconds);

            // Per-hour time-series bucket (HGETALL used by resolveTimeSeries)
            jedis.hincrBy(tsKey, String.valueOf(bucket), 1L);
            jedis.expire(tsKey, ttlSeconds);
        } catch (Exception ex) {
            log.warn("Redis write failed for hashKey={} tsKey={}: {}",
                    hashKey, tsKey, ex.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            log.info("RedisHotCounterSink JedisPool closed");
        }
    }
}
