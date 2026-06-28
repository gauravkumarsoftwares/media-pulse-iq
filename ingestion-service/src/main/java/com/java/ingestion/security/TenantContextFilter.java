package com.java.ingestion.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Edge multi-tenancy guard (architecture 6.1) and MDC correlation-ID injector (OB-2).
 *
 * <p><strong>Tenant enforcement</strong>: ensures every ingest request carries an
 * {@code X-Tenant-Context} claim (injected by the gateway after PASETO validation).
 * Requests to the ingest path without it are rejected early with {@code 401} before
 * hitting the controller.
 *
 * <p><strong>MDC injection (OB-2)</strong>: for every request this filter sets:
 * <ul>
 *   <li>{@code requestId} — taken from {@code X-Request-Id} header if present,
 *       otherwise a fresh UUID is generated. The value is also echoed in the
 *       response as {@code X-Request-Id} so clients can correlate logs.</li>
 *   <li>{@code tenantId} — set for event-ingest paths where the tenant is known.</li>
 * </ul>
 * Both values are cleared in the {@code finally} block to prevent thread-pool leaks.
 */
@Component
@Order(1)
public final class TenantContextFilter extends OncePerRequestFilter {

    public static final String TENANT_HEADER    = "X-Tenant-Context";
    public static final String TENANT_ATTRIBUTE = "tenantContext";
    public static final String REQUEST_ID_HEADER = "X-Request-Id";

    private static final String INGEST_PATH_PREFIX = "/api/v1/events";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        // ---- OB-2: MDC request correlation ID ----------------------------------
        String requestId = request.getHeader(REQUEST_ID_HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }
        MDC.put("requestId", requestId);
        // Echo back so API clients can correlate their logs with ours.
        response.setHeader(REQUEST_ID_HEADER, requestId);

        try {
            if (request.getRequestURI().startsWith(INGEST_PATH_PREFIX)) {
                String tenant = request.getHeader(TENANT_HEADER);
                if (tenant == null || tenant.isBlank()) {
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    response.setContentType("application/json");
                    response.getWriter().write(
                            "{\"error\":\"Missing " + TENANT_HEADER + ". Authentication required.\"}");
                    return;
                }
                MDC.put("tenantId", tenant);
                request.setAttribute(TENANT_ATTRIBUTE, tenant);
            }
            filterChain.doFilter(request, response);
        } finally {
            // Must clear MDC before thread returns to pool (OB-2 leak prevention).
            MDC.remove("requestId");
            MDC.remove("tenantId");
        }
    }
}


