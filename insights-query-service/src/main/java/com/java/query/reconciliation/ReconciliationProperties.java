package com.java.query.reconciliation;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Externalized configuration for the periodic reconciliation jobs.
 *
 * <p>All properties are bound from the {@code platform.reconciliation.*} namespace.
 * Sensible defaults are provided so the feature works out-of-the-box with no
 * additional configuration; tune per-environment via profile YAMLs.
 *
 * <h2>Example application.yml</h2>
 * <pre>
 * platform:
 *   reconciliation:
 *     enabled: true
 *     discrepancy-threshold-pct: 1.0
 *     max-campaigns-per-run: 5000
 *     hourly-cron: "0 5 * * * *"   # 5 past the hour
 *     daily-cron:  "0 15 1 * * *"  # 01:15 UTC every day
 * </pre>
 */
@Component
@ConfigurationProperties(prefix = "platform.reconciliation")
public class ReconciliationProperties {

    /** Master toggle — set to {@code false} to disable all reconciliation jobs. */
    private boolean enabled = true;

    /**
     * Percentage threshold above which a discrepancy is flagged and alerted.
     * E.g. {@code 1.0} means flag when |observed - reference| / reference > 1 %.
     */
    private double discrepancyThresholdPct = 1.0;

    /**
     * Maximum number of (tenantId, campaignId, eventType) triplets processed in
     * a single run.  Guards against unexpectedly large result sets from Pinot.
     */
    private int maxCampaignsPerRun = 5_000;

    /**
     * Cron expression for the hourly hot-tier job (Redis vs Pinot).
     * Default: 5 minutes past every hour.
     */
    private String hourlyCron = "0 5 * * * *";

    /**
     * Cron expression for the daily warm-tier job (Pinot vs Iceberg).
     * Default: 01:15 UTC every day — well after midnight segment flush.
     */
    private String dailyCron = "0 15 1 * * *";

    /**
     * When {@code true}, the hourly job automatically patches Redis counters that
     * are lower than the corresponding Pinot count for the same window.
     * Set to {@code false} in staging/dev to observe-only mode.
     */
    private boolean autoCorrectRedis = true;

    // ---- generated accessors -----------------------------------------------

    public boolean isEnabled()                    { return enabled; }
    public void    setEnabled(boolean v)          { this.enabled = v; }

    public double  getDiscrepancyThresholdPct()         { return discrepancyThresholdPct; }
    public void    setDiscrepancyThresholdPct(double v) { this.discrepancyThresholdPct = v; }

    public int     getMaxCampaignsPerRun()          { return maxCampaignsPerRun; }
    public void    setMaxCampaignsPerRun(int v)     { this.maxCampaignsPerRun = v; }

    public String  getHourlyCron()                  { return hourlyCron; }
    public void    setHourlyCron(String v)          { this.hourlyCron = v; }

    public String  getDailyCron()                   { return dailyCron; }
    public void    setDailyCron(String v)           { this.dailyCron = v; }

    public boolean isAutoCorrectRedis()             { return autoCorrectRedis; }
    public void    setAutoCorrectRedis(boolean v)   { this.autoCorrectRedis = v; }
}

