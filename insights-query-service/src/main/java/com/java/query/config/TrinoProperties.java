package com.java.query.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Externalised configuration for the Trino/Iceberg cold-tier connection (architecture 4.3).
 *
 * <p>Bound from {@code platform.trino.*} in application[-profile].yml.
 *
 * <h3>Connection pooling</h3>
 * <p>A HikariCP pool is created when {@link #enabled} is {@code true}.
 * Pool settings ({@link #maxPoolSize}, {@link #minIdle}, {@link #connectionTimeoutMs},
 * {@link #idleTimeoutMs}) should be tuned for the number of concurrent cold-tier queries
 * expected.  Cold queries are rare (window &gt;30 days) but each can take several seconds,
 * so keep the pool small (default: 10) to avoid queueing on the Trino coordinator.
 *
 * <h3>JDBC URL format</h3>
 * <pre>
 *   jdbc:trino://&lt;host&gt;:&lt;port&gt;/&lt;catalog&gt;/&lt;schema&gt;
 *   e.g. jdbc:trino://trino-coordinator:8080/iceberg/ads
 * </pre>
 *
 * <h3>Authentication</h3>
 * <p>Trino uses the {@code X-Trino-User} header for identity — set via {@link #user}.
 * Password-based auth (LDAP / file) is enabled by also providing {@link #password}.
 * For TLS: set {@link #sslEnabled} and configure the truststore path.
 * Credentials are <strong>never</strong> committed — inject via K8s Secrets
 * ({@code TRINO_USER} / {@code TRINO_PASSWORD}) through {@code app-secrets} envFrom.
 */
@Component
@ConfigurationProperties(prefix = "platform.trino")
@Getter
@Setter
public class TrinoProperties {

    /**
     * Whether the Trino cold-tier client is active.
     * {@code false} (default) in local/dev — queries fall through to the in-memory StarTree store.
     */
    private boolean enabled = false;

    /**
     * Full Trino JDBC URL, e.g. {@code jdbc:trino://trino:8080/iceberg/ads}.
     * The URL must include catalog and schema so no additional session-level
     * {@code USE} commands are needed (HikariCP cannot replay them on reconnect).
     */
    private String jdbcUrl = "jdbc:trino://trino:8080/iceberg/ads";

    /**
     * Trino user name — sent as the {@code X-Trino-User} header.
     * Must map to a valid user in the Trino file-based authenticator or LDAP.
     * Injected from {@code TRINO_USER} environment variable (K8s Secret).
     */
    private String user = "insights-query-service";

    /**
     * Trino password for LDAP / password-file authentication.
     * Leave blank for clusters where user-header auth is sufficient.
     * Injected from {@code TRINO_PASSWORD} environment variable (K8s Secret).
     * <p><strong>Never commit this value to source control.</strong>
     */
    private String password = "";

    /**
     * Iceberg catalog name as registered in the Trino metastore.
     * Used to build fully-qualified table references: {@code catalog.schema.table}.
     */
    private String catalog = "iceberg";

    /**
     * Iceberg schema (namespace) inside the catalog.
     */
    private String schema = "ads";

    /**
     * Iceberg table name within the schema.
     * Must match the Flink/Iceberg table configured in the Trino metastore.
     */
    private String table = "shopping_events";

    /**
     * Per-query timeout in seconds (mapped to {@code Statement.setQueryTimeout}).
     * Prevents a slow Trino scan from blocking the reconciliation job indefinitely.
     * Default: 60 seconds (cold Iceberg scans can take several seconds on large partitions).
     */
    private int queryTimeoutSeconds = 60;

    // ---- HikariCP pool configuration ----------------------------------------

    /**
     * Maximum number of connections in the HikariCP pool.
     * Cold queries are infrequent and slow; a small pool is intentional.
     */
    private int maxPoolSize = 10;

    /**
     * Minimum number of idle connections to keep in the pool.
     */
    private int minIdle = 2;

    /**
     * Maximum time (ms) a caller will wait to acquire a connection before throwing.
     * Default: 30 seconds — aligned with query timeout to avoid double-waiting.
     */
    private long connectionTimeoutMs = 30_000;

    /**
     * Maximum time (ms) a connection may sit idle in the pool before being evicted.
     * Trino coordinator does not maintain persistent sessions; idle eviction prevents stale
     * connections from accumulating. Default: 10 minutes.
     */
    private long idleTimeoutMs = 600_000;

    /**
     * How frequently (ms) HikariCP pings idle connections to keep them alive.
     * Must be less than {@link #idleTimeoutMs}. Default: 5 minutes.
     */
    private long keepaliveTimeMs = 300_000;

    // ---- TLS configuration --------------------------------------------------

    /**
     * Enable TLS/HTTPS for the Trino JDBC connection (append {@code ?SSL=true} to URL).
     * Required for staging/prod clusters with TLS termination.
     */
    private boolean sslEnabled = false;

    /**
     * Path to the Java truststore (.jks) used to verify the Trino coordinator TLS certificate.
     * Only required when {@link #sslEnabled} is {@code true} and the cert is not in the
     * default JVM truststore.
     */
    private String sslTruststorePath = "";

    /**
     * Truststore password. Injected from {@code TRINO_SSL_TRUSTSTORE_PASSWORD} K8s Secret.
     */
    private String sslTruststorePassword = "";
}

