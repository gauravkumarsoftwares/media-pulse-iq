package com.java.query.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientTransport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Infrastructure beans for the insights-query-service that are not managed
 * by Spring Boot auto-configuration.
 *
 * <h3>Why {@code JsonAsyncHttpPinotClientTransportFactory}?</h3>
 * <p>The native Apache Pinot Java SDK ({@code pinot-java-client}) is used instead
 * of a plain {@link org.springframework.web.client.RestClient} for several reasons:
 * <ul>
 *   <li><strong>Async I/O</strong> — the transport layer uses Netty via
 *       {@code async-http-client}, giving non-blocking HTTP with a proper
 *       connection pool instead of blocking JDK sockets.</li>
 *   <li><strong>Broker failover</strong> — {@link ConnectionFactory#fromBrokerList}
 *       distributes queries across multiple broker replicas and retries on
 *       broker-level failures automatically.</li>
 *   <li><strong>Native result types</strong> — {@link org.apache.pinot.client.ResultSet}
 *       provides typed column accessors ({@code getString}, {@code getLong}, …)
 *       without manual JSON parsing overhead.</li>
 *   <li><strong>Pinot exception types</strong> — {@link org.apache.pinot.client.PinotClientException}
 *       carries Pinot-specific error codes, enabling richer error handling.</li>
 *   <li><strong>Built-in auth support</strong> — extra HTTP headers (Basic or Bearer)
 *       are injected once at transport construction and applied to every request.</li>
 * </ul>
 */
@Configuration
@Slf4j
public class PlatformInfraConfig {

    // =========================================================================
    //  Apache Pinot — warm-tier (query window < 30 days)
    // =========================================================================

    /**
     * Shared Apache Pinot {@link Connection} for warm-tier queries.
     *
     * <p>The connection is lazily established — no socket is opened until the first
     * query is executed, so the bean is safe to create even when
     * {@code platform.pinot.enabled=false} (local/dev).
     *
     * <p>Spring calls {@link Connection#close()} when the application context shuts
     * down ({@code destroyMethod = "close"}), releasing the underlying Netty
     * event-loop threads and connection pool.
     *
     * @param props bound from {@code platform.pinot.*}
     * @return a configured, auth-aware Pinot {@link Connection}
     */
    @Bean(destroyMethod = "close")
    public Connection pinotConnection(PinotProperties props) {
        // Configure the async transport: scheme, auth headers, and timeouts.
        // JsonAsyncHttpPinotClientTransportFactory uses setters (not a fluent builder)
        // and accepts timeout overrides through a Properties object.
        JsonAsyncHttpPinotClientTransportFactory factory =
                new JsonAsyncHttpPinotClientTransportFactory();

        factory.setScheme(props.getScheme());
        factory.setHeaders(buildAuthHeaders(props));

        // Override default timeouts via the property keys the factory understands.
        Properties connProps = new Properties();
        connProps.setProperty("brokerConnectTimeoutMs",
                String.valueOf(props.getConnectTimeoutMs()));
        connProps.setProperty("brokerReadTimeoutMs",
                String.valueOf(props.getReadTimeoutMs()));
        factory.withConnectionProperties(connProps);

        PinotClientTransport transport = factory.buildTransport();

        // fromBrokerList accepts plain "host:port" strings;
        // the scheme is configured on the transport above.
        return ConnectionFactory.fromHostList(List.of(props.getBroker()), transport);
    }

    /**
     * Builds the {@code Authorization} header map based on the configured
     * {@link PinotProperties#getAuthScheme()}.
     *
     * <table border="1">
     *   <tr><th>authScheme</th><th>Header added</th></tr>
     *   <tr><td>NONE</td><td>— (no header)</td></tr>
     *   <tr><td>BASIC</td><td>{@code Authorization: Basic <base64(user:pass)>}</td></tr>
     *   <tr><td>TOKEN</td><td>{@code Authorization: Bearer <token>}</td></tr>
     * </table>
     *
     * <p>If the required credentials are blank for the selected scheme, no header is added
     * so that misconfiguration surfaces as a 401 from Pinot rather than a silent failure.
     */
    private static Map<String, String> buildAuthHeaders(PinotProperties props) {
        Map<String, String> headers = new HashMap<>();
        switch (props.getAuthScheme().toUpperCase()) {
            case "BASIC" -> {
                if (!props.getUsername().isBlank() && !props.getPassword().isBlank()) {
                    String credentials = props.getUsername() + ":" + props.getPassword();
                    headers.put("Authorization", "Basic " + Base64.getEncoder()
                            .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
                }
            }
            case "TOKEN" -> {
                if (!props.getAuthToken().isBlank()) {
                    headers.put("Authorization", "Bearer " + props.getAuthToken());
                }
            }
            default -> { /* NONE — no Authorization header */ }
        }
        return headers;
    }

    // =========================================================================
    //  Trino / Iceberg — cold-tier (query window > 30 days)
    // =========================================================================

    /**
     * HikariCP connection pool for Trino JDBC cold-tier queries.
     *
     * <p>Only created when {@code platform.trino.enabled=true} (staging/prod).
     * In local/dev the bean is absent and {@link com.java.query.store.TrinoIcebergClient}
     * short-circuits to return empty results, falling back to the in-memory StarTree store.
     *
     * <h3>Pool sizing rationale</h3>
     * <p>Cold queries are rare (window &gt;30 days) and each query already exploits
     * Trino's internal parallelism across workers.  A small pool (default: 10) is
     * intentional to avoid overwhelming the Trino coordinator with concurrent sessions.
     *
     * <h3>HikariCP + Trino notes</h3>
     * <ul>
     *   <li>{@code initializationFailTimeout = -1} — the pool does not fail at Spring
     *       startup if the Trino coordinator is temporarily unavailable; it retries
     *       on the first actual query.</li>
     *   <li>{@code connectionTestQuery = "SELECT 1"} — Trino JDBC supports this
     *       lightweight validation query.</li>
     *   <li>Catalog and schema embedded in the JDBC URL avoid per-connection
     *       {@code USE} statements that HikariCP cannot replay on reconnect.</li>
     * </ul>
     *
     * @param props bound from {@code platform.trino.*}
     * @return a configured HikariCP {@link DataSource} for Trino JDBC
     */
    @Bean(name = "trinoDataSource", destroyMethod = "close")
    @ConditionalOnProperty(prefix = "platform.trino", name = "enabled", havingValue = "true")
    public DataSource trinoDataSource(TrinoProperties props) {
        HikariConfig config = new HikariConfig();

        // JDBC URL must include catalog/schema: jdbc:trino://host:port/catalog/schema
        config.setJdbcUrl(buildTrinoJdbcUrl(props));
        config.setUsername(props.getUser());
        if (!props.getPassword().isBlank()) {
            config.setPassword(props.getPassword());
        }
        config.setDriverClassName("io.trino.jdbc.TrinoDriver");

        // Pool settings — keep small; cold queries are rare and internally parallel.
        config.setMaximumPoolSize(props.getMaxPoolSize());
        config.setMinimumIdle(props.getMinIdle());
        config.setConnectionTimeout(props.getConnectionTimeoutMs());
        config.setIdleTimeout(props.getIdleTimeoutMs());
        config.setKeepaliveTime(props.getKeepaliveTimeMs());
        config.setPoolName("trino-iceberg-pool");

        // Do not block startup if Trino coordinator is temporarily unavailable.
        config.setInitializationFailTimeout(-1L);

        // Lightweight validation query supported by Trino JDBC.
        config.setConnectionTestQuery("SELECT 1");

        log.info("[trino] Creating HikariCP pool → url={} user={} maxPool={}",
                config.getJdbcUrl(), props.getUser(), props.getMaxPoolSize());
        return new HikariDataSource(config);
    }

    /**
     * Builds the Trino JDBC URL from {@link TrinoProperties}, appending SSL and
     * application-name parameters when required.
     *
     * <p>The base URL is expected to include catalog and schema, e.g.:
     * {@code jdbc:trino://trino-coordinator:8080/iceberg/ads}
     */
    private static String buildTrinoJdbcUrl(TrinoProperties props) {
        StringBuilder url = new StringBuilder(props.getJdbcUrl());
        String sep = url.toString().contains("?") ? "&" : "?";

        url.append(sep).append("applicationName=insights-query-service");

        if (props.isSslEnabled()) {
            url.append("&SSL=true");
            if (!props.getSslTruststorePath().isBlank()) {
                url.append("&SSLTrustStorePath=").append(props.getSslTruststorePath());
                if (!props.getSslTruststorePassword().isBlank()) {
                    url.append("&SSLTrustStorePassword=").append(props.getSslTruststorePassword());
                }
            }
        }
        return url.toString();
    }
}
