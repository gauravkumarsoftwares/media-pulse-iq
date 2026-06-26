package com.java.ingestion.security;

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
 * In-service PASETO authentication (OWASP A01/A07 hardening). Supports both
 * {@code v4.public} (direct edge-token mode) and {@code v4.local}
 * (token-exchange / Option C) via the pluggable {@link PasetoVerifier}.
 *
 * <p>Runs ahead of {@link TenantContextFilter} ({@code @Order(0)}). When
 * verification is enabled it cryptographically verifies the token, enforces the
 * {@code write:events} scope required for the ingest path, and derives the tenant
 * from the <em>verified claims</em> — it does not trust a client-supplied
 * {@code X-Tenant-Context}. The validated tenant is re-injected as
 * {@code X-Tenant-Context} (via a request wrapper) so downstream components keep
 * a single, uniform contract. The full {@link PasetoClaims} are exposed as a
 * request attribute for authorization.
 *
 * <p>When the verifier is absent (local dev, auth disabled) the filter is a
 * pass-through and the legacy gateway-header trust applies.
 */
@Component
@Order(0)
@Slf4j
public final class PasetoAuthenticationFilter extends OncePerRequestFilter {

    public static final String CLAIMS_ATTRIBUTE = "pasetoClaims";

    /** Dedicated audit stream for SIEM ingestion (OWASP A09). */
    private static final Logger AUDIT = LoggerFactory.getLogger("SECURITY_AUDIT");
    private static final String TENANT_HEADER = "X-Tenant-Context";
    private static final String WRITE_SCOPE = "write:events";

    private final PasetoVerifier verifier; // null when disabled
    private final PasetoProperties props;

    public PasetoAuthenticationFilter(@Nullable PasetoVerifier verifier,
                                      PasetoProperties props) {
        this.verifier = verifier;
        this.props = props;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // Never gate health/readiness probes.
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
                unauthorized(response, "Token missing tenant_id");
                return;
            }
            if (!claims.hasScope(WRITE_SCOPE)) {
                AUDIT.warn("event=authz_denied reason=missing_scope tenant={} path={}",
                        claims.tenantId(), request.getRequestURI());
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\":\"Missing required scope: " + WRITE_SCOPE + "\"}");
                return;
            }
            request.setAttribute(CLAIMS_ATTRIBUTE, claims);
            AUDIT.info("event=auth_success tenant={} path={}", claims.tenantId(), request.getRequestURI());
            // Force the trusted tenant downstream regardless of client headers.
            filterChain.doFilter(new TenantOverrideRequest(request, claims.tenantId()), response);
        } catch (PasetoException ex) {
            AUDIT.warn("event=auth_denied reason=invalid_token path={}", request.getRequestURI());
            log.debug("PASETO verification failed: {}", ex.getMessage());
            unauthorized(response, "Invalid or missing authentication token");
        }
    }

    private void unauthorized(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + message + "\"}");
    }

    /** Overrides {@code X-Tenant-Context} with the verified tenant id. */
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

