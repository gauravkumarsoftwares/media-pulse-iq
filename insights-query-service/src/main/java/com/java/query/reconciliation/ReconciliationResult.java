package com.java.query.reconciliation;

/**
 * Immutable result of a single campaign-key comparison during a reconciliation run.
 *
 * @param key                 the campaign dimension key compared
 * @param referenceStore      name of the authoritative (ground-truth) store
 *                            ({@code "iceberg"} for daily, {@code "pinot"} for hourly)
 * @param referenceCount      count obtained from the reference store
 * @param observedStore       name of the store being validated
 *                            ({@code "pinot"} for daily, {@code "redis"} for hourly)
 * @param observedCount       count obtained from the observed store
 * @param delta               {@code observedCount - referenceCount}
 * @param discrepancyPct      {@code |delta| / referenceCount * 100}, or 0 when referenceCount == 0
 * @param autoPatched         {@code true} when the reconciler automatically corrected the value
 *                            (only applicable to the Redis hot-tier patch in the hourly job)
 */
public record ReconciliationResult(
        CampaignKey key,
        String      referenceStore,
        long        referenceCount,
        String      observedStore,
        long        observedCount,
        long        delta,
        double      discrepancyPct,
        boolean     autoPatched
) {

    /** Returns {@code true} when |delta| / referenceCount exceeds the supplied threshold. */
    public boolean exceedsThreshold(double thresholdPct) {
        return discrepancyPct > thresholdPct;
    }

    /** Convenience factory — computes delta and discrepancyPct automatically. */
    public static ReconciliationResult of(CampaignKey key,
                                          String referenceStore, long referenceCount,
                                          String observedStore,  long observedCount,
                                          boolean autoPatched) {
        long delta = observedCount - referenceCount;
        double pct = (referenceCount == 0)
                ? (observedCount == 0 ? 0.0 : 100.0)
                : Math.abs(delta) * 100.0 / referenceCount;
        return new ReconciliationResult(key, referenceStore, referenceCount,
                observedStore, observedCount, delta, pct, autoPatched);
    }
}

