package com.java.security.paseto;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Duration;

/**
 * Shared Spring Boot auto-configuration for PASETO in-service verification.
 *
 * <p>Registered via
 * {@code META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports}
 * in {@code shared-security} so every consuming service picks up the
 * {@link PasetoVerifier} and key-guard beans automatically — no {@code @Import} needed.
 *
 * <p>Only activates in servlet web applications ({@code @ConditionalOnWebApplication}).
 * Flink or other non-servlet consumers of {@code shared-security} are unaffected.
 *
 * <h3>Fail-closed</h3>
 * <p>If PASETO is enabled but the required key for the configured mode is missing,
 * the {@code pasetoKeyGuard} {@link ApplicationRunner} throws
 * {@link IllegalStateException} at startup — preventing a silent auth bypass (OWASP A05/A07).
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(PasetoProperties.class)
@Slf4j
public class PasetoSecurityConfig {

    private static final String MODE_LOCAL = "local";

    /**
     * Creates a {@link PasetoVerifier} bean based on {@code platform.security.paseto.mode}.
     * Returns {@code null} when auth is disabled (local dev) so
     * {@link AbstractPasetoAuthenticationFilter#shouldNotFilter} bypasses every request.
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

    /** Fail-closed startup guard: aborts if PASETO is enabled but the key is missing. */
    @Bean
    public ApplicationRunner pasetoKeyGuard(PasetoProperties props) {
        return args -> {
            if (!props.isEnabled()) return;
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

