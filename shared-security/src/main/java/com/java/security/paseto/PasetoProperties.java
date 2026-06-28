package com.java.security.paseto;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds {@code platform.security.paseto.*} configuration for all services.
 *
 * <p>Registered via {@link PasetoSecurityConfig} ({@code @EnableConfigurationProperties}).
 * Services do <strong>not</strong> need to declare their own copy.
 *
 * <p>When {@link #enabled} is {@code true} the service performs full in-service
 * PASETO v4.public / v4.local verification (defense-in-depth) rather than blindly
 * trusting the gateway-injected {@code X-Tenant-Context} header.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "platform.security.paseto")
public class PasetoProperties {

    /** Enable in-service token verification. Disable only for local dev. */
    private boolean enabled = false;

    /**
     * Token flavour to verify in-service:
     * <ul>
     *   <li>{@code public} — verify the external {@code v4.public} token (Ed25519).</li>
     *   <li>{@code local}  — verify the gateway-minted internal {@code v4.local}
     *       token (token-exchange / Option C).</li>
     * </ul>
     */
    private String mode = "public";

    /** Ed25519 public key (PEM / X.509 base64 / raw hex|base64) — used when mode=public. */
    private String publicKey;

    /** 32-byte symmetric key (hex/base64) for v4.local — used when mode=local. */
    private String localKey;

    /** Expected issuer ("iss"); empty = not enforced. */
    private String issuer;

    /** Expected audience ("aud"); empty = not enforced. */
    private String audience;

    /** Header carrying the token (gateway-forwarded). Default: Authorization. */
    private String tokenHeader = "Authorization";

    /** Allowed clock skew in seconds. Default: 60. */
    private long clockSkewSeconds = 60;
}

