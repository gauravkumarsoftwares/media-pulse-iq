package com.java.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.java.ingestion.common.ApiException;
import com.java.ingestion.dto.IngestEventRequest;
import com.java.ingestion.dto.IngestEventResponse;
import com.java.ingestion.security.PasetoProperties;
import com.java.ingestion.service.IngestionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Web-layer tests for {@link IngestController}:
 * tenant enforcement, @Valid DTO validation, service delegation, and error envelopes.
 */
@WebMvcTest(IngestController.class)
@Import({PasetoProperties.class,
        com.java.ingestion.common.GlobalExceptionHandler.class})
class IngestControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockitoBean IngestionService ingestionService;

    // ---- missing tenant → 401 ----------------------------------------------

    @Test
    @DisplayName("Rejects ingest without X-Tenant-Context header (401)")
    void rejectsMissingTenant() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validPayload()))
                .andExpect(status().isUnauthorized());

        verifyNoInteractions(ingestionService);
    }

    // ---- @Valid: blank required field → 422 --------------------------------

    @Test
    @DisplayName("Returns 422 when required field eventId is blank")
    void rejectsBlankEventId() throws Exception {
        IngestEventRequest bad = new IngestEventRequest(
                "", "u", "s", "c", "CLICK", 0, 0.0, null);

        mockMvc.perform(post("/api/v1/events")
                        .header("X-Tenant-Context", "walmart_us")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(bad)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.fieldErrors").isArray());

        verifyNoInteractions(ingestionService);
    }

    // ---- service returns 400 for bad eventType → propagated ----------------

    @Test
    @DisplayName("Returns 400 when service rejects an unrecognized eventType")
    void rejectsInvalidEventType() throws Exception {
        when(ingestionService.ingest(any(), eq("walmart_us")))
                .thenThrow(new ApiException(HttpStatus.BAD_REQUEST,
                        "Unrecognized or missing eventType: NOT_A_TYPE"));

        IngestEventRequest req = new IngestEventRequest(
                "e1", "u", "s", null, "NOT_A_TYPE", 0, 0.0, null);

        mockMvc.perform(post("/api/v1/events")
                        .header("X-Tenant-Context", "walmart_us")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(req)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    // ---- happy path → 202 --------------------------------------------------

    @Test
    @DisplayName("Returns 202 Accepted and delegates to IngestionService on valid request")
    void acceptsValidEvent() throws Exception {
        IngestEventResponse resp = new IngestEventResponse(
                "ACCEPTED", "e2", Instant.now().toString(), 499L);
        when(ingestionService.ingest(any(), eq("walmart_us"))).thenReturn(resp);

        mockMvc.perform(post("/api/v1/events")
                        .header("X-Tenant-Context", "walmart_us")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validPayload()))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.eventId").value("e2"));

        verify(ingestionService).ingest(any(), eq("walmart_us"));
    }

    // ---- service throws 429 → propagated -----------------------------------

    @Test
    @DisplayName("Returns 429 when service signals rate-limit exceeded")
    void rateLimitPropagated() throws Exception {
        when(ingestionService.ingest(any(), eq("busy_tenant")))
                .thenThrow(new ApiException(HttpStatus.TOO_MANY_REQUESTS,
                        "Rate limit exceeded."));

        mockMvc.perform(post("/api/v1/events")
                        .header("X-Tenant-Context", "busy_tenant")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validPayload()))
                .andExpect(status().isTooManyRequests());
    }

    // ---- helpers -----------------------------------------------------------

    private String validPayload() throws Exception {
        IngestEventRequest req = new IngestEventRequest(
                "e2", "u", "s", "camp1", "CLICK", 0, 0.0, null);
        return objectMapper.writeValueAsString(req);
    }
}
