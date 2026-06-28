package com.java.query.controller;

import com.java.query.common.ApiException;
import com.java.query.dto.CampaignMetricResponse;
import com.java.query.dto.TimeSeriesPoint;
import com.java.security.paseto.PasetoProperties;
import com.java.query.service.InsightsService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Web-layer tests for {@link AdInsightsController}:
 * tenant enforcement, typed DTO responses, and service exception propagation.
 */
@WebMvcTest(AdInsightsController.class)
@Import({PasetoProperties.class,
        com.java.query.common.GlobalExceptionHandler.class})
class AdInsightsControllerTest {

    @Autowired MockMvc mockMvc;

    @MockitoBean InsightsService insightsService;

    // ---- missing tenant → 401 ----------------------------------------------

    @Test
    @DisplayName("Returns 401 when X-Tenant-Context header is missing")
    void unauthorizedWithoutTenant() throws Exception {
        mockMvc.perform(get("/api/v1/campaigns/camp1/clicks"))
                .andExpect(status().isUnauthorized());
    }

    // ---- happy path → 200 with typed response ------------------------------

    @Test
    @DisplayName("Returns campaign clicks with typed CampaignMetricResponse")
    void returnsClicks() throws Exception {
        CampaignMetricResponse resp = new CampaignMetricResponse(
                "walmart_us", "camp1", "click",
                Instant.now().minusSeconds(3600).toString(), Instant.now().toString(),
                "hour", null,
                List.of(new TimeSeriesPoint(Instant.now().toString(), 42L)),
                42L, 5L, "redis-hot");

        when(insightsService.getMetrics(
                eq("walmart_us"), isNull(), eq("camp1"), eq("CLICK"),
                isNull(), isNull(), eq("hour"), isNull()))
                .thenReturn(resp);

        mockMvc.perform(get("/api/v1/campaigns/camp1/clicks")
                        .header("X-Tenant-Context", "walmart_us"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tenantId").value("walmart_us"))
                .andExpect(jsonPath("$.campaignId").value("camp1"))
                .andExpect(jsonPath("$.metric").value("click"))
                .andExpect(jsonPath("$.total").value(42))
                .andExpect(jsonPath("$.series").isArray());
    }

    // ---- service throws 400 for bad grain → propagated via GlobalExceptionHandler

    @Test
    @DisplayName("Returns 400 when service rejects an invalid grain")
    void invalidGrainReturns400() throws Exception {
        when(insightsService.getMetrics(any(), any(), any(), any(), any(), any(), eq("weekly"), any()))
                .thenThrow(new ApiException(HttpStatus.BAD_REQUEST,
                        "Invalid grain. Allowed: minute, hour, day."));

        mockMvc.perform(get("/api/v1/campaigns/camp1/clicks")
                        .header("X-Tenant-Context", "walmart_us")
                        .param("grain", "weekly"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").exists());
    }

    // ---- service throws 403 for restricted campaign → propagated -----------

    @Test
    @DisplayName("Returns 403 when campaign is outside allowed_campaigns")
    void campaignNotAllowedReturns403() throws Exception {
        when(insightsService.getMetrics(any(), any(), eq("restricted"), any(), any(), any(), any(), any()))
                .thenThrow(new ApiException(HttpStatus.FORBIDDEN,
                        "Not authorized for campaign: restricted"));

        mockMvc.perform(get("/api/v1/campaigns/restricted/clicks")
                        .header("X-Tenant-Context", "walmart_us"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.message").exists());
    }
}
