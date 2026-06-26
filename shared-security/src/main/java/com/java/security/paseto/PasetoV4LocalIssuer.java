package com.java.security.paseto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Mints internal PASETO {@code v4.local} tokens — the edge/token-exchange step
 * of Option C.
 *
 * <p>After the gateway verifies the external {@code v4.public} token, it calls
 * {@link #issue} to produce a short-lived internal token (narrowed audience,
 * tiny TTL) that carries the already-validated claims into the mesh. Services
 * verify it with {@link PasetoV4LocalVerifier}. This keeps the external token
 * out of the internal network entirely.
 *
 * <p>Reference implementation: in production this logic lives at the edge
 * (a Kong plugin or a thin token-exchange service); it is provided here so the
 * platform — and its tests — can mint internal tokens with the shared library.
 */
public final class PasetoV4LocalIssuer {

    private final byte[] key;
    private final String issuer;
    private final String audience;
    private final ObjectMapper mapper = new ObjectMapper();

    public PasetoV4LocalIssuer(String keyMaterial, String issuer, String audience) {
        this.key = SymmetricKeys.parse32(keyMaterial);
        this.issuer = issuer;
        this.audience = audience;
    }

    /**
     * Issue an internal token for a verified principal.
     *
     * @param tenantId         the verified tenant
     * @param scopes           granted scopes (e.g. {@code read:ads})
     * @param allowedCampaigns optional per-campaign allow-list (may be null/empty)
     * @param ttl              token lifetime (keep very short, e.g. 30–60s)
     */
    public String issue(String tenantId,
                        List<String> scopes,
                        List<String> allowedCampaigns,
                        Duration ttl) {
        Instant now = Instant.now();
        ObjectNode claims = mapper.createObjectNode();
        claims.put("tenant_id", tenantId);
        if (issuer != null) {
            claims.put("iss", issuer);
        }
        if (audience != null) {
            claims.put("aud", audience);
        }
        claims.put("iat", now.toString());
        claims.put("nbf", now.toString());
        claims.put("exp", now.plus(ttl).toString());
        if (scopes != null) {
            claims.putPOJO("scopes", scopes);
        }
        if (allowedCampaigns != null) {
            claims.putPOJO("allowed_campaigns", allowedCampaigns);
        }

        try {
            byte[] json = mapper.writeValueAsBytes(claims);
            return PasetoV4Local.encrypt(key, json);
        } catch (Exception ex) {
            throw new PasetoException("Unable to mint v4.local token", ex);
        }
    }

    /** Convenience for callers that already have a JSON claims document. */
    public String issueRaw(String claimsJson) {
        return PasetoV4Local.encrypt(key, claimsJson.getBytes(StandardCharsets.UTF_8));
    }
}

