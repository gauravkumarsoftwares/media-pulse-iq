package com.java.ingestion.security;

import com.java.security.paseto.AbstractPasetoAuthenticationFilter;
import com.java.security.paseto.PasetoProperties;
import com.java.security.paseto.PasetoVerifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Ingestion-service PASETO filter — enforces the {@code write:events} scope.
 *
 * <p>All common authentication logic (token verification, tenant injection,
 * audit logging, {@code TenantOverrideRequest}) lives in
 * {@link AbstractPasetoAuthenticationFilter} in {@code shared-security}.
 */
@Component
@Order(0)
@Slf4j
public final class PasetoAuthenticationFilter extends AbstractPasetoAuthenticationFilter {

    public PasetoAuthenticationFilter(@Nullable PasetoVerifier verifier,
                                      PasetoProperties props) {
        super(verifier, props);
    }

    @Override
    protected String requiredScope() {
        return "write:events";
    }
}
