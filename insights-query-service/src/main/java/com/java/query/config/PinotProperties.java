package com.java.query.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Externalised configuration for the Apache Pinot broker connection (architecture 4).
 *
 * <p>Bound from {@code platform.pinot.*} in application[-profile].yml.
 *
 * <h3>Authentication</h3>
 * <p>The Pinot broker REST API supports three auth modes controlled by {@link #authScheme}:
 * <ul>
 *   <li>{@code NONE}  — no {@code Authorization} header (open cluster, local/dev)</li>
 *   <li>{@code BASIC} — HTTP Basic auth; set {@link #username} + {@link #password}
 *       (injected from {@code PINOT_USERNAME} / {@code PINOT_PASSWORD} K8s Secrets)</li>
 *   <li>{@code TOKEN} — Bearer token; set {@link #authToken}
 *       (injected from {@code PINOT_AUTH_TOKEN} K8s Secret)</li>
 * </ul>
 * Credentials are <strong>never</strong> committed — they are delivered as K8s Secrets
 * via External Secrets Operator / Vault and injected through {@code app-secrets} envFrom.
 */
@Component
@ConfigurationProperties(prefix = "platform.pinot")
@Getter
@Setter
public class PinotProperties {

    /**
     * Pinot broker host:port, e.g. {@code pinot-broker:8099}.
     * Used by the native Pinot Java client when opening a connection.
     */
    private String broker = "localhost:8099";

    /**
     * HTTP scheme for the Pinot broker connection: {@code http} (default) or {@code https}.
     * Set to {@code https} when the broker is TLS-terminated (staging/prod).
     */
    private String scheme = "http";

    /**
     * Name of the Pinot realtime table that holds the enriched shopping events.
     * Must match the table name configured in the Pinot cluster.
     */
    private String table = "shopping_events";

    /**
     * HTTP connect timeout in milliseconds for Pinot REST queries.
     */
    private int connectTimeoutMs = 3_000;

    /**
     * HTTP read (socket) timeout in milliseconds for Pinot REST queries.
     */
    private int readTimeoutMs = 10_000;

    /**
     * When {@code true}, Pinot is available and queries will be attempted.
     * Set to {@code false} in local/dev profiles to fall back to the in-memory
     * {@link com.java.query.store.StarTreeStore} without connection errors.
     */
    private boolean enabled = false;

    // ---- Authentication -------------------------------------------------

    /**
     * Authentication scheme for the Pinot broker REST API.
     * <ul>
     *   <li>{@code NONE}  — no auth header (default; local / open clusters)</li>
     *   <li>{@code BASIC} — HTTP Basic auth using {@link #username} + {@link #password}</li>
     *   <li>{@code TOKEN} — Bearer token auth using {@link #authToken}</li>
     * </ul>
     */
    private String authScheme = "NONE";

    /**
     * Pinot broker username for {@code BASIC} auth.
     * Injected from the {@code PINOT_USERNAME} environment variable (K8s Secret).
     * Leave blank when {@link #authScheme} is not {@code BASIC}.
     */
    private String username = "";

    /**
     * Pinot broker password for {@code BASIC} auth.
     * Injected from the {@code PINOT_PASSWORD} environment variable (K8s Secret).
     * Leave blank when {@link #authScheme} is not {@code BASIC}.
     * <p><strong>Never commit this value to source control.</strong>
     */
    private String password = "";

    /**
     * Bearer token for {@code TOKEN} auth (e.g. a PASETO / JWT issued by Pinot's
     * own identity provider or an external IdP).
     * Injected from the {@code PINOT_AUTH_TOKEN} environment variable (K8s Secret).
     * Leave blank when {@link #authScheme} is not {@code TOKEN}.
     * <p><strong>Never commit this value to source control.</strong>
     */
    private String authToken = "";
}
