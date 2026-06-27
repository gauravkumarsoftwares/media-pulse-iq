package com.java.query.router;

/**
 * Serving tiers for the hybrid read path (architecture 5.2). Each tier maps to
 * a backing store and the {@code source} label surfaced in API responses.
 */
public enum QueryTier {

    /** Hot, high-frequency counters for the last ~48 hours. */
    REDIS_CACHE("RedisCache"),

    /** Real-time OLAP aggregates for the last ~30 days. */
    PINOT_OLAP("ApachePinot"),

    /** Cold, federated historical scans beyond 30 days. */
    TRINO_LAKEHOUSE("TrinoLakehouse");

    private final String sourceLabel;

    QueryTier(String sourceLabel) {
        this.sourceLabel = sourceLabel;
    }

    /** @return the human-facing source label for API responses. */
    public String sourceLabel() {
        return sourceLabel;
    }
}

