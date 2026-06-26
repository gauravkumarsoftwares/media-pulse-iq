package com.java.query.security;

import com.java.security.paseto.PasetoClaims;
import com.java.security.paseto.PasetoException;
import com.java.security.paseto.PasetoVerifier;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;

/**
 * In-service PASETO authentication for the read path (OWASP A01/A07). Supports
 * both {@code v4.public} (direct edge-token mode) and {@code v4.local}
 * (token-exchange / Option C) via the pluggable {@link PasetoVerifier}.
 * Verifies the token, enforces the {@code read:ads} scope, and injects the
 * verified tenant as {@code X-Tenant-Context}. Per-campaign authorization
 * (the {@code allowed_campaigns} claim) is enforced in the controller, where
 * the campaign id is known.
 */
@Component
@Order(0)
@Slf4j
public final class PasetoAuthenticationFilter extends OncePerRequestFilter {

    public static final String CLAIMS_ATTRIBUTE = "pasetoClaims";

    /** Dedicated audit stream for SIEM ingestion (OWASP A09). */
    private static final Logger AUDIT = LoggerFactory.getLogger("SECURITY_AUDIT");
    private static final String TENANT_HEADER = "X-Tenant-Context";
    private static final String READ_SCOPE = "read:ads";

    private final PasetoVerifier verifier; // null when disabled
    private final PasetoProperties props;

    public PasetoAuthenticationFilter(@Nullable PasetoVerifier verifier,
                                      PasetoProperties props) {
        this.verifier = verifier;
        this.props = props;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return verifier == null || uri.startsWith("/actuator");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String token = request.getHeader(props.getTokenHeader());
        try {
            PasetoClaims claims = verifier.verify(token);
            if (claims.tenantId() == null || claims.tenantId().isBlank()) {
                AUDIT.warn("event=auth_denied reason=missing_tenant path={}", request.getRequestURI());
                deny(response, HttpServletResponse.SC_UNAUTHORIZED, "Token missing tenant_id");
                return;
            }
            if (!claims.hasScope(READ_SCOPE)) {
                AUDIT.warn("event=authz_denied reason=missing_scope tenant={} path={}",
                        claims.tenantId(), request.getRequestURI());
                deny(response, HttpServletResponse.SC_FORBIDDEN, "Missing required scope: " + READ_SCOPE);
                return;
            }
            request.setAttribute(CLAIMS_ATTRIBUTE, claims);
            AUDIT.info("event=auth_success tenant={} path={}", claims.tenantId(), request.getRequestURI());
            filterChain.doFilter(new TenantOverrideRequest(request, claims.tenantId()), response);
        } catch (PasetoException ex) {
            AUDIT.warn("event=auth_denied reason=invalid_token path={}", request.getRequestURI());
            log.debug("PASETO verification failed: {}", ex.getMessage());
            deny(response, HttpServletResponse.SC_UNAUTHORIZED, "Invalid or missing authentication token");
        }
    }

    private void deny(HttpServletResponse response, int status, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + message + "\"}");
    }

    private static final class TenantOverrideRequest extends HttpServletRequestWrapper {
        private final String tenantId;

        TenantOverrideRequest(HttpServletRequest request, String tenantId) {
            super(request);
            this.tenantId = tenantId;
        }

        @Override
        public String getHeader(String name) {
            if (TENANT_HEADER.equalsIgnoreCase(name)) {
                return tenantId;
            }
            return super.getHeader(name);
        }

        @Override
        public Enumeration<String> getHeaders(String name) {
            if (TENANT_HEADER.equalsIgnoreCase(name)) {
                return Collections.enumeration(Collections.singletonList(tenantId));
            }
            return super.getHeaders(name);
        }
    }
}

