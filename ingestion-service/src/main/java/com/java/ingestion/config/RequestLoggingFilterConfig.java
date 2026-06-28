package com.java.ingestion.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.CommonsRequestLoggingFilter;

/**
 * API-level access log configuration (architecture 8 — observability).
 *
 * <p>Registers a {@link CommonsRequestLoggingFilter} that writes a
 * structured log line for every inbound HTTP request <em>before</em> and
 * <em>after</em> the controller processes it.  The "after" message includes
 * the full request URI, the caller's remote address, selected headers
 * (including the {@code X-Tenant-Context} and {@code X-Request-Id}
 * correlation headers), and — for POST/PUT requests — up to 4 KB of
 * the request body so event-ingestion payloads are traceable.
 *
 * <h3>Activation</h3>
 * Log output is gated by the logger level of
 * {@code org.springframework.web.filter.CommonsRequestLoggingFilter}.
 * Set it to {@code DEBUG} (already done in {@code application.yml}) to
 * activate it; set it to {@code INFO} or higher to silence it in production.
 *
 * <h3>Tomcat access log</h3>
 * Low-level per-request access logs (IP, status, bytes, elapsed ms) are
 * written by the embedded Tomcat engine and configured separately via
 * {@code server.tomcat.accesslog.*} in {@code application.yml}.
 */
@Configuration
public class RequestLoggingFilterConfig {

    /**
     * Maximum request-body snippet captured in the "after" log entry.
     * Large enough to cover a typical ingest payload; small enough to
     * avoid excessive log volume.
     */
    private static final int MAX_PAYLOAD_LENGTH = 4096;

    @Bean
    public CommonsRequestLoggingFilter requestLoggingFilter() {

        CommonsRequestLoggingFilter filter = new CommonsRequestLoggingFilter();

        // Include the full URI (path + query string) in every log entry.
        filter.setIncludeQueryString(true);

        // Include the caller's remote address for traceability.
        filter.setIncludeClientInfo(true);

        // Include request headers so tenant context and correlation IDs
        // appear in every log entry without needing MDC instrumentation.
        filter.setIncludeHeaders(true);

        // Capture the request body for write-path requests (POST /api/v1/events).
        // Spring's filter buffers the stream so the controller can still read it.
        filter.setIncludePayload(true);
        filter.setMaxPayloadLength(MAX_PAYLOAD_LENGTH);

        // Human-readable prefix tokens in the log message.
        filter.setBeforeMessagePrefix("[ACCESS] >> ");
        filter.setAfterMessagePrefix("[ACCESS] << ");

        return filter;
    }
}

