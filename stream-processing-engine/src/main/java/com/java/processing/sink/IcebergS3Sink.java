package com.java.processing.sink;

import com.java.model.ShoppingEvent;

/**
 * Abstraction for the Apache Iceberg / S3 data lake sink (architecture 1, 3.3).
 *
 * <h2>Architectural role</h2>
 * Flink appends every raw, deduplicated event to Apache Iceberg as the
 * <strong>source-of-truth store</strong> for:
 * <ul>
 *   <li>Cold-tier historical queries (>30 d) via Trino federated SQL</li>
 *   <li>Nightly billing reconciliation — exact deduplication over the full
 *       row-level log corrects any Pinot approximations (9.1)</li>
 *   <li>Data-sovereignty compliance — Iceberg partitions by {@code tenant_id}
 *       and {@code event_date} so data for a specific region can be deleted or
 *       exported independently (GDPR right-to-erasure)</li>
 *   <li>Late-event correction — extremely late events (>5 min watermark) are
 *       upserted into Iceberg retroactively without rewriting Pinot segments</li>
 * </ul>
 *
 * <h2>Production implementation</h2>
 * Use the <em>Apache Iceberg Flink connector</em> (artifact:
 * {@code org.apache.iceberg:iceberg-flink-runtime-1.19}):
 * <pre>{@code
 * FlinkSink.forRowData(uniqueStream.map(toRowData))
 *     .tableLoader(TableLoader.fromCatalog(catalog, TableIdentifier.of("ads", "shopping_events")))
 *     .distributionMode(DistributionMode.HASH)
 *     .writeParallelism(parallelism)
 *     .build();
 * }</pre>
 *
 * <p>The reference build uses {@link IcebergS3SinkStub} which logs the write intent
 * without an actual Iceberg cluster dependency. Swap in the real connector when
 * deploying to AWS/GCP/Azure.
 */
public interface IcebergS3Sink {

    /**
     * Append a raw deduplicated event to the Iceberg table.
     * In production this batches rows and flushes per Iceberg file-format.
     *
     * @param event the unique, enriched event to persist in the data lake
     */
    void append(ShoppingEvent event);
}

