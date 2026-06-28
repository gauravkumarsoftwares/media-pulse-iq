package com.java.query.service;

import com.java.query.common.ApiException;
import com.java.security.paseto.PasetoClaims;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Extracted input validation and per-campaign authorization (B5 — SRP).
 *
 * <p>Previously embedded in {@link InsightsServiceImpl}, these two concerns
 * are now a dedicated, independently testable component. The service calls
 * {@link #validate} once and delegates all error-throwing here.
 */
@Component
@Slf4j
public class InsightsRequestValidator {

    /** Allow-list charset + max length (OWASP A03/A04). */
    private static final Pattern    ID_PATTERN   = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");
    private static final Set<String> VALID_GRAINS = Set.of("minute", "hour", "day");

    /**
     * Validate request parameters and enforce per-campaign authorization.
     *
     * @param tenantId   verified tenant from the filter
     * @param campaignId campaign identifier from the path variable
     * @param grain      time-bucket grain from the query param
     * @param placement  optional placement filter
     * @param claims     verified PASETO claims (may be {@code null} when auth is disabled)
     * @throws ApiException on any validation or authorization failure
     */
    public void validate(String tenantId, String campaignId,
                         String grain, String placement,
                         PasetoClaims claims) {

        if (!ID_PATTERN.matcher(campaignId).matches()) {
            log.warn("[VALIDATOR] invalid campaignId tenant={} campaignId={}", tenantId, campaignId);
            throw new ApiException(HttpStatus.BAD_REQUEST, "Invalid campaignId format.");
        }
        if (placement != null && !placement.isBlank()
                && !ID_PATTERN.matcher(placement).matches()) {
            log.warn("[VALIDATOR] invalid placement tenant={} placement={}", tenantId, placement);
            throw new ApiException(HttpStatus.BAD_REQUEST, "Invalid placement format.");
        }
        if (!VALID_GRAINS.contains(grain)) {
            log.warn("[VALIDATOR] invalid grain tenant={} grain={}", tenantId, grain);
            throw new ApiException(HttpStatus.BAD_REQUEST,
                    "Invalid grain. Allowed: minute, hour, day.");
        }
        if (claims != null && !claims.canAccessCampaign(campaignId)) {
            log.warn("[VALIDATOR] authorization denied tenant={} campaign={}", tenantId, campaignId);
            throw new ApiException(HttpStatus.FORBIDDEN,
                    "Not authorized for campaign: " + campaignId);
        }
    }
}

