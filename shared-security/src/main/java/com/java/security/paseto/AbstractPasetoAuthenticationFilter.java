package com.java.security.paseto;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;

/**
 * Shared base filter for in-service PASETO authentication (OWASP A01/A07).
 *
 * <p>Supports both {@code v4.public} (direct edge-token) and {@code v4.local}
 * (token-exchange / Option C) via the pluggable {@link PasetoVerifier}.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>Verifies the PASETO token from the header defined by {@link PasetoProperties#getTokenHeader()}.</li>
 *   <li>Enforces a non-blank {@code tenant_id} claim.</li>
 *   <li>Enforces the scope returned by {@link #requiredScope()} — subclasses declare
 *       their service-specific scope ({@code write:events}, {@code read:ads}, …).</li>
 *   <li>Re-injects the verified tenant as {@code X-Tenant-Context} via
 *       {@link TenantOverrideRequest} so downstream components keep a single contract.</li>
 * </ol>
 *
 * <h3>Bypass</h3>
 * <p>When {@code verifier} is {@code null} (auth disabled in local dev) or the URI
 * starts with {@code /actuator}, the filter is a no-op pass-through.
 *
 * <h3>Audit logging</h3>
 * <p>Security events are written to the {@code SECURITY_AUDIT} named logger for
 * SIEM ingestion (OWASP A09), independent of the application log level.
 *
 * <p>Concrete subclasses need only declare {@code @Component @Order(0)} and
 * implement {@link #requiredScope()}.
 */
@Slf4j
public abstract class AbstractPasetoAuthenticationFilter extends OncePerRequestFilter {

    /** Request attribute key under which the verified {@link PasetoClaims} are stored. */
    public static final String CLAIMS_ATTRIBUTE = "pasetoClaims";

    /** Dedicated audit stream for SIEM ingestion (OWASP A09). */
    private static final Logger AUDIT = LoggerFactory.getLogger("SECURITY_AUDIT");
    private static final String TENANT_HEADER = "X-Tenant-Context";

    private final PasetoVerifier verifier; // null when auth is disabled
    private final PasetoProperties props;

    protected AbstractPasetoAuthenticationFilter(@Nullable PasetoVerifier verifier,
                                                  PasetoProperties props) {
        this.verifier = verifier;
        this.props    = props;
    }

    /**
     * The OAuth 2.0-style scope string this service path requires.
     * Examples: {@code "write:events"} (ingestion), {@code "read:ads"} (query).
     */
    protected abstract String requiredScope();

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        // Skip when auth is disabled (local dev) or for health/readiness probes.
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
            if (!claims.hasScope(requiredScope())) {
                AUDIT.warn("event=authz_denied reason=missing_scope tenant={} path={}",
                        claims.tenantId(), request.getRequestURI());
                deny(response, HttpServletResponse.SC_FORBIDDEN,
                        "Missing required scope: " + requiredScope());
                return;
            }

            request.setAttribute(CLAIMS_ATTRIBUTE, claims);
            AUDIT.info("event=auth_success tenant={} path={}", claims.tenantId(), request.getRequestURI());
            // Force the verified tenant downstream regardless of any client-supplied header.
            filterChain.doFilter(new TenantOverrideRequest(request, claims.tenantId()), response);

        } catch (PasetoException ex) {
            AUDIT.warn("event=auth_denied reason=invalid_token path={}", request.getRequestURI());
            log.debug("PASETO verification failed: {}", ex.getMessage());
            deny(response, HttpServletResponse.SC_UNAUTHORIZED,
                    "Invalid or missing authentication token");
        }
    }

    /**
     * Write a JSON error response and set the HTTP status.
     *
     * @param response HTTP response
     * @param status   HTTP status code (e.g. 401, 403)
     * @param message  human-readable error message
     */
    protected final void deny(HttpServletResponse response, int status, String message)
            throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        response.getWriter().write("{\"error\":\"" + message + "\"}");
    }

    // ---- Inner request wrapper -----------------------------------------------

    /**
     * Overrides {@code X-Tenant-Context} with the cryptographically verified tenant id,
     * preventing clients from spoofing the header downstream.
     */
    private static final class TenantOverrideRequest extends HttpServletRequestWrapper {

        private final String tenantId;

        TenantOverrideRequest(HttpServletRequest request, String tenantId) {
            super(request);
            this.tenantId = tenantId;
        }

        @Override
        public String getHeader(String name) {
            if (TENANT_HEADER.equalsIgnoreCase(name)) return tenantId;
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

