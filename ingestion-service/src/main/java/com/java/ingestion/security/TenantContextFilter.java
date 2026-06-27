package com.java.ingestion.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Edge multi-tenancy guard (architecture 6.1).
 *
 * <p>Ensures every ingest request carries an {@code X-Tenant-Context} claim
 * (injected by the gateway after PASETO validation). Requests to the ingest
 * path without it are rejected early with {@code 401} before hitting the
 * controller. The validated tenant is exposed as a request attribute for
 * downstream components.
 *
 * <p>This is intentionally defense-in-depth: the controller also re-checks the
 * header so unit tests and non-filtered invocations remain safe.
 */
@Component
@Order(1)
public final class TenantContextFilter extends OncePerRequestFilter {

    public static final String TENANT_HEADER = "X-Tenant-Context";
    public static final String TENANT_ATTRIBUTE = "tenantContext";

    private static final String INGEST_PATH_PREFIX = "/api/v1/events";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        if (request.getRequestURI().startsWith(INGEST_PATH_PREFIX)) {
            String tenant = request.getHeader(TENANT_HEADER);
            if (tenant == null || tenant.isBlank()) {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                response.setContentType("application/json");
                response.getWriter().write(
                        "{\"error\":\"Missing " + TENANT_HEADER + ". Authentication required.\"}");
                return;
            }
            request.setAttribute(TENANT_ATTRIBUTE, tenant);
        }
        filterChain.doFilter(request, response);
    }
}

