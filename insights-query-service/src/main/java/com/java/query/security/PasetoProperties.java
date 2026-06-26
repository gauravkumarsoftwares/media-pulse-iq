package com.java.query.security;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Binds {@code platform.security.paseto.*} for the insights query service.
 * See the ingestion service's equivalent for field semantics.
 */
@Component
@Getter
@Setter
@ConfigurationProperties(prefix = "platform.security.paseto")
public class PasetoProperties {

    private boolean enabled = false;
    private String mode = "public";   // "public" (v4.public) | "local" (v4.local token-exchange)
    private String publicKey;         // used when mode=public
    private String localKey;          // 32-byte symmetric key (hex/base64), used when mode=local
    private String issuer;
    private String audience;
    private String tokenHeader = "Authorization";
    private long clockSkewSeconds = 60;
}
