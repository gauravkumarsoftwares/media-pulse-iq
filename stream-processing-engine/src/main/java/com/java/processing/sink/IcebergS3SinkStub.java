package com.java.processing.sink;

import com.java.model.ShoppingEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Reference-build stub for {@link IcebergS3Sink} (architecture §1, §3.3).
 *
 * <p>Logs the write intent so the data-lake path is observable in local/dev mode.
 * In production, replace with the Apache Iceberg Flink connector
 * ({@code org.apache.iceberg:iceberg-flink-runtime-1.19}) wired into
 * {@link com.java.processing.job.FlinkStreamingJob}.
 *
 * <p>Active only when {@code platform.iceberg.enabled=false} (default).
 * When {@code enabled=true} the real connector takes over.
 */
@Component
@ConditionalOnProperty(name = "platform.iceberg.enabled", havingValue = "false", matchIfMissing = true)
@Slf4j
public class IcebergS3SinkStub implements IcebergS3Sink {

    /**
     * In production: batch-write to Parquet files under
     * {@code s3://<bucket>/warehouse/ads.db/shopping_events/tenant_id=<t>/event_date=<d>/}.
     * Iceberg handles ACID commits, schema evolution, and partition pruning.
     */
    @Override
    public void append(ShoppingEvent event) {
        // Stub: emit an audit log so the data-lake write path is visible in traces.
        // Production: FlinkSink.forRowData(...).tableLoader(icebergTableLoader).build()
        log.debug("[ICEBERG-STUB] Would append event={} tenant={} type={} ts={}",
                event.getEventId(), event.getTenantId(),
                event.getEventType(), event.getEventTimestampMs());
    }
}

