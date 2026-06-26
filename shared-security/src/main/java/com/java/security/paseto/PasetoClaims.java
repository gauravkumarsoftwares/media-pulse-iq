package com.java.security.paseto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * The verified claims carried by a platform PASETO token. Drives both
 * authentication (who: {@link #tenantId()}) and authorization
 * ({@link #scopes()}, {@link #allowedCampaigns()}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PasetoClaims(
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("scopes") List<String> scopes,
        @JsonProperty("allowed_campaigns") List<String> allowedCampaigns,
        @JsonProperty("iss") String issuer,
        @JsonProperty("aud") String audience,
        @JsonProperty("sub") String subject,
        @JsonProperty("exp") String expiration,
        @JsonProperty("nbf") String notBefore,
        @JsonProperty("iat") String issuedAt,
        @JsonProperty("jti") String tokenId) {

    /** True if the token grants the given scope (e.g. {@code read:ads}). */
    public boolean hasScope(String scope) {
        return scopes != null && scopes.contains(scope);
    }

    /**
     * True if the token may access the given campaign. An absent/empty
     * {@code allowed_campaigns} claim means "no campaign restriction" (only the
     * tenant-level scoping applies).
     */
    public boolean canAccessCampaign(String campaignId) {
        return allowedCampaigns == null
                || allowedCampaigns.isEmpty()
                || allowedCampaigns.contains(campaignId);
    }

    public Instant expirationInstant() {
        return parse(expiration);
    }

    public Instant notBeforeInstant() {
        return parse(notBefore);
    }

    private static Instant parse(String iso) {
        if (iso == null || iso.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(iso);
        } catch (Exception ex) {
            throw new PasetoException("Invalid timestamp claim: " + iso);
        }
    }
}
