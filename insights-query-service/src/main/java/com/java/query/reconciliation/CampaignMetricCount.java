package com.java.query.reconciliation;

/**
 * A single row from the Pinot GROUP BY query used to discover active campaigns
 * and their event counts for a given time window.
 *
 * <p>Produced by {@link com.java.query.store.PinotRestClient#queryGroupedCounts}.
 */
public record CampaignMetricCount(CampaignKey key, long count) { }

