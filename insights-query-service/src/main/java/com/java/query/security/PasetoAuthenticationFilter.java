package com.java.query.security;

import com.java.security.paseto.AbstractPasetoAuthenticationFilter;
import com.java.security.paseto.PasetoProperties;
import com.java.security.paseto.PasetoVerifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * Query-service PASETO filter — enforces the {@code read:ads} scope.
 *
 * <p>Per-campaign authorization (the {@code allowed_campaigns} claim) is enforced
 * downstream in {@link com.java.query.service.InsightsRequestValidator} where the
 * campaign id is available.
 *
 * <p>All common filter logic lives in {@link AbstractPasetoAuthenticationFilter}
 * in {@code shared-security}.
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
        return "read:ads";
    }
}
