package com.java.processing.sink;

import com.java.model.ShoppingEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Flink {@link RichSinkFunction} that increments campaign hot-counters in Redis
 * using atomic {@code HINCRBY} operations (architecture 4, hot tier &lt;48 h).
 *
 * <p><strong>Key schema:</strong> {@code campaign:{tenantId}:{campaignId}}
 * <br><strong>Hash field:</strong> {@code eventType} (CLICK, IMPRESSION, CLICK_TO_BASKET …)
 * <br><strong>TTL:</strong> configurable via {@code platform.flink.redis-ttl-seconds} (default 48 h).
 *
 * <p>The {@link JedisPool} is {@code transient} and created in {@link #open(Configuration)} so
 * it is never included in Flink's operator checkpoint serialisation.  This class is safe for
 * distributed Flink execution where each TaskManager constructs its own pool.
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

        String key   = "campaign:" + event.getTenantId() + ":" + event.getCampaignId();
        String field = event.getEventType();

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hincrBy(key, field, 1L);
            // Refresh TTL on every write to keep active campaigns hot.
            jedis.expire(key, ttlSeconds);
        } catch (Exception ex) {
            // Non-fatal: log and continue — Pinot is the durable store.
            log.warn("Redis HINCRBY failed for key={} field={}: {}", key, field, ex.getMessage());
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

