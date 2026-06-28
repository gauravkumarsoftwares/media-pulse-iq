package com.java.query.reconciliation;

import com.java.query.common.ApiException;
import com.java.query.dto.ReconciliationDetailDto;
import com.java.query.dto.ReconciliationRunSummary;
import com.java.security.paseto.PasetoProperties;
import com.java.query.service.ReconciliationQueryService;
import com.java.query.common.PagedResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Web-layer tests for {@link ReconciliationController}.
 */
@WebMvcTest(ReconciliationController.class)
@Import({PasetoProperties.class,
        com.java.query.common.GlobalExceptionHandler.class})
class ReconciliationControllerTest {

    @Autowired MockMvc mockMvc;

    @MockitoBean ReconciliationQueryService reconciliationService;

    // ---- GET /status -------------------------------------------------------

    @Test
    @DisplayName("GET /api/v1/reconciliation/status returns NO_RUN_YET when store is empty")
    void status_noRunYet() throws Exception {
        when(reconciliationService.getStatus())
                .thenReturn(Map.of(
                        "hourly", noRunYet(),
                        "daily",  noRunYet()));

        mockMvc.perform(get("/api/v1/reconciliation/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.hourly.status").value("NO_RUN_YET"))
                .andExpect(jsonPath("$.daily.status").value("NO_RUN_YET"));
    }

    @Test
    @DisplayName("GET /api/v1/reconciliation/status returns summary when a report exists")
    void status_withReport() throws Exception {
        when(reconciliationService.getStatus())
                .thenReturn(Map.of(
                        "hourly", okEntry(),
                        "daily",  noRunYet()));

        mockMvc.perform(get("/api/v1/reconciliation/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.hourly.status").value("OK"))
                .andExpect(jsonPath("$.hourly.campaigns").value(0))
                .andExpect(jsonPath("$.daily.status").value("NO_RUN_YET"));
    }

    // ---- GET /reports/{window} ---------------------------------------------

    @Test
    @DisplayName("GET /api/v1/reconciliation/reports/hourly returns 200 with paged list")
    void listReports_hourly() throws Exception {
        when(reconciliationService.listReports("hourly", 10))
                .thenReturn(PagedResponse.of(List.of(summary())));

        mockMvc.perform(get("/api/v1/reconciliation/reports/hourly"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.items").isArray())
                .andExpect(jsonPath("$.total").value(1));
    }

    @Test
    @DisplayName("GET /api/v1/reconciliation/reports/weekly returns 400")
    void listReports_invalidWindow() throws Exception {
        when(reconciliationService.listReports(eq("weekly"), anyInt()))
                .thenThrow(new ApiException(HttpStatus.BAD_REQUEST,
                        "Unknown window type 'weekly'. Use 'hourly' or 'daily'."));

        mockMvc.perform(get("/api/v1/reconciliation/reports/weekly"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    // ---- GET /reports/{window}/latest -------------------------------------

    @Test
    @DisplayName("GET .../latest returns NO_RUN_YET when no run has been done")
    void latestReport_noRunYet() throws Exception {
        when(reconciliationService.getLatestReport("daily")).thenReturn(null);

        mockMvc.perform(get("/api/v1/reconciliation/reports/daily/latest"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("NO_RUN_YET"));
    }

    @Test
    @DisplayName("GET .../latest returns the full detail DTO when a report exists")
    void latestReport_withData() throws Exception {
        when(reconciliationService.getLatestReport("daily")).thenReturn(detail());

        mockMvc.perform(get("/api/v1/reconciliation/reports/daily/latest"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK"))
                .andExpect(jsonPath("$.results").isArray());
    }

    // ---- POST /runs/{window} -----------------------------------------------

    @Test
    @DisplayName("POST /api/v1/reconciliation/runs/hourly triggers a run and returns 200")
    void triggerRun_hourly() throws Exception {
        when(reconciliationService.triggerRun("hourly")).thenReturn(detail());

        mockMvc.perform(post("/api/v1/reconciliation/runs/hourly"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("OK"));

        verify(reconciliationService).triggerRun("hourly");
    }

    @Test
    @DisplayName("POST /api/v1/reconciliation/runs/unknown returns 400")
    void triggerRun_invalidWindow() throws Exception {
        when(reconciliationService.triggerRun("yearly"))
                .thenThrow(new ApiException(HttpStatus.BAD_REQUEST,
                        "Unknown window type 'yearly'. Use 'hourly' or 'daily'."));

        mockMvc.perform(post("/api/v1/reconciliation/runs/yearly"))
                .andExpect(status().isBadRequest());
    }

    // ---- helpers -----------------------------------------------------------

    private static ReconciliationRunSummary noRunYet() {
        return new ReconciliationRunSummary(null, "NO_RUN_YET", null, null, null, 0, 0, 0, 0, null);
    }

    private static ReconciliationRunSummary okEntry() {
        return new ReconciliationRunSummary("run-1", "OK",
                "2026-06-26T01:00:00Z", "2026-06-25T23:00:00Z", "2026-06-26T01:00:00Z",
                0, 0, 0, 100L, null);
    }

    private static ReconciliationRunSummary summary() {
        return new ReconciliationRunSummary("run-1", "OK",
                "2026-06-26T01:00:00Z", null, null, 0, 0, 0, 100L, null);
    }

    private static ReconciliationDetailDto detail() {
        return new ReconciliationDetailDto("run-1", "OK",
                "2026-06-26T01:00:00Z", null, null, 0, 0, 0, 100L, null, List.of());
    }
}
