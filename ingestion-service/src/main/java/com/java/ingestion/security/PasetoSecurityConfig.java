package com.java.ingestion.security;

import com.java.security.paseto.PasetoV4LocalVerifier;
import com.java.security.paseto.PasetoV4PublicVerifier;
import com.java.security.paseto.PasetoVerifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Security wiring for the ingestion service.
 *
 * <p>Selects the in-service verifier by {@code platform.security.paseto.mode}:
 * {@code public} (external Ed25519 token) or {@code local} (gateway-minted
 * {@code v4.local} internal token — token-exchange / Option C).
 *
 * <p><strong>Fail-closed:</strong> if PASETO is enabled but the key required for
 * the configured mode is missing, the application refuses to start (prevents a
 * silent authentication bypass — OWASP A05/A07).
 */
@Configuration
@Slf4j
public class PasetoSecurityConfig {

    private static final String MODE_LOCAL = "local";

    /**
     * Builds the verifier only when auth is enabled <em>and</em> the mode's key
     * exists. Returns {@code null} when disabled (local dev) so the filter passes
     * through.
     */
    @Bean
    public PasetoVerifier pasetoVerifier(PasetoProperties props) {
        if (!props.isEnabled()) {
            log.warn("PASETO verification DISABLED — trusting gateway X-Tenant-Context header. "
                    + "This must only happen in local development.");
            return null;
        }
        Duration skew = Duration.ofSeconds(props.getClockSkewSeconds());
        if (MODE_LOCAL.equalsIgnoreCase(props.getMode())) {
            log.info("PASETO in-service verification enabled (mode=local, v4.local token-exchange).");
            return new PasetoV4LocalVerifier(
                    props.getLocalKey(), props.getIssuer(), props.getAudience(), skew);
        }
        log.info("PASETO in-service verification enabled (mode=public, v4.public).");
        return new PasetoV4PublicVerifier(
                props.getPublicKey(), props.getIssuer(), props.getAudience(), skew);
    }

    /** Fail-closed startup guard. */
    @Bean
    public ApplicationRunner pasetoKeyGuard(PasetoProperties props) {
        return args -> {
            if (!props.isEnabled()) {
                return;
            }
            boolean local = MODE_LOCAL.equalsIgnoreCase(props.getMode());
            String key = local ? props.getLocalKey() : props.getPublicKey();
            if (key == null || key.isBlank()) {
                throw new IllegalStateException(
                        "PASETO is enabled (mode=" + props.getMode() + ") but the required key "
                                + (local ? "platform.security.paseto.local-key"
                                         : "platform.security.paseto.public-key")
                                + " is missing — refusing to start (fail-closed).");
            }
        };
    }
}
