package com.java.query.controller;

import com.java.query.dto.CampaignMetricResponse;
import com.java.query.security.PasetoAuthenticationFilter;
import com.java.query.service.InsightsService;
import com.java.security.paseto.PasetoClaims;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Campaign Insights REST API (architecture 5.1).
 *
 * <p>URI prefix: {@code /api/v1/campaigns}. Deliberately thin — all validation,
 * authorization, tier routing, and query execution are delegated to
 * {@link InsightsService}. The controller only extracts the tenant identity
 * and hands off to the service.
 */
@RestController
@RequestMapping("/api/v1/campaigns")
@RequiredArgsConstructor
@Tag(name = "Campaign Insights", description = "Campaign metric time-series and aggregates")
public final class AdInsightsController {

    private final InsightsService insightsService;

    // ---- clicks -------------------------------------------------------------

    @GetMapping("/{campaignId}/clicks")
    @Operation(summary = "Get click metrics for a campaign")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Metric data returned"),
            @ApiResponse(responseCode = "400", description = "Invalid input parameters"),
            @ApiResponse(responseCode = "401", description = "Missing tenant context"),
            @ApiResponse(responseCode = "403", description = "Campaign not in allowed_campaigns"),
    })
    public ResponseEntity<CampaignMetricResponse> clicks(
            @PathVariable String campaignId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "hour") String grain,
            @RequestParam(required = false) String placement,
            @RequestHeader(value = "X-Tenant-Context", required = false) String tenant,
            @RequestAttribute(value = PasetoAuthenticationFilter.CLAIMS_ATTRIBUTE, required = false)
            PasetoClaims claims) {
        return respond(tenant, claims, campaignId, "CLICK", from, to, grain, placement);
    }

    // ---- impressions --------------------------------------------------------

    @GetMapping("/{campaignId}/impressions")
    @Operation(summary = "Get impression metrics for a campaign")
    public ResponseEntity<CampaignMetricResponse> impressions(
            @PathVariable String campaignId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "hour") String grain,
            @RequestParam(required = false) String placement,
            @RequestHeader(value = "X-Tenant-Context", required = false) String tenant,
            @RequestAttribute(value = PasetoAuthenticationFilter.CLAIMS_ATTRIBUTE, required = false)
            PasetoClaims claims) {
        return respond(tenant, claims, campaignId, "IMPRESSION", from, to, grain, placement);
    }

    // ---- click-to-basket ----------------------------------------------------

    @GetMapping("/{campaignId}/click-to-basket")
    @Operation(summary = "Get click-to-basket metrics for a campaign")
    public ResponseEntity<CampaignMetricResponse> clickToBasket(
            @PathVariable String campaignId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(defaultValue = "hour") String grain,
            @RequestParam(required = false) String placement,
            @RequestHeader(value = "X-Tenant-Context", required = false) String tenant,
            @RequestAttribute(value = PasetoAuthenticationFilter.CLAIMS_ATTRIBUTE, required = false)
            PasetoClaims claims) {
        return respond(tenant, claims, campaignId, "CLICK_TO_BASKET", from, to, grain, placement);
    }

    // ---- shared dispatch ----------------------------------------------------

    private ResponseEntity<CampaignMetricResponse> respond(
            String tenant, PasetoClaims claims,
            String campaignId, String metricType,
            String from, String to, String grain, String placement) {

        if (tenant == null || tenant.isBlank()) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        // All validation, authorization, and querying in the service layer.
        CampaignMetricResponse response = insightsService.getMetrics(
                tenant, claims, campaignId, metricType, from, to, grain, placement);
        return ResponseEntity.ok(response);
    }
}
