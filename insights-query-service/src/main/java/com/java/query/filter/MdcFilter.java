package com.java.query.filter;

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
 * MDC correlation-ID injector for the insights-query-service (OB-2).
 *
 * <p>Populates SLF4J MDC with {@code requestId} and {@code tenantId} so that every
 * log line emitted during a request includes the correlation context. This enables
 * log aggregation tools (Loki / ELK) to group all log lines for a single request.
 *
 * <ul>
 *   <li>{@code requestId} — taken from {@code X-Request-Id} header if present,
 *       otherwise a fresh UUID is generated and echoed back in the response.</li>
 *   <li>{@code tenantId} — taken from {@code X-Tenant-Context} header (optional;
 *       not enforced here — security enforcement is handled by
 *       {@link com.java.query.security.PasetoAuthenticationFilter}).</li>
 * </ul>
 *
 * <p>Both keys are removed in the {@code finally} block to prevent thread-pool leaks.
 */
@Component
@Order(1)
public final class MdcFilter extends OncePerRequestFilter {

    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    public static final String TENANT_HEADER     = "X-Tenant-Context";

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {

        String requestId = request.getHeader(REQUEST_ID_HEADER);
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString();
        }
        MDC.put("requestId", requestId);
        response.setHeader(REQUEST_ID_HEADER, requestId);   // echo back for client correlation

        String tenant = request.getHeader(TENANT_HEADER);
        if (tenant != null && !tenant.isBlank()) {
            MDC.put("tenantId", tenant);
        }

        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove("requestId");
            MDC.remove("tenantId");
        }
    }
}

