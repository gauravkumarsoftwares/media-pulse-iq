package com.java.model.constants;

/**
 * Centralised Kafka topic name <em>templates</em> and naming-convention reference.
 *
 * <h2>Naming Convention</h2>
 * <pre>
 * {env}.{visibility}.{topic-type}.{domain}.{subdomain}.{record-name}-by-{key-name}[-v{N}]
 * </pre>
 *
 * <table border="1">
 *   <caption>Component definitions</caption>
 *   <tr><th>Component</th><th>Values / Notes</th></tr>
 *   <tr><td>{env}</td><td>local | dev | staging | prod  (driven by KAFKA_ENV env-var)</td></tr>
 *   <tr><td>{visibility}</td>
 *       <td>external – third-party exchange; shared – cross-domain internal;
 *           internal – same domain; private – single application</td></tr>
 *   <tr><td>{topic-type}</td>
 *       <td>event | command | entity | cdc | notification</td></tr>
 *   <tr><td>{domain}.{subdomain}</td><td>DDD bounded-context hierarchy</td></tr>
 *   <tr><td>{record-name}</td>
 *       <td>Events → Noun + PastTense; Commands → PresentTense + Noun</td></tr>
 *   <tr><td>{key-name}</td><td>The Kafka partition key field</td></tr>
 *   <tr><td>[-v{N}]</td><td>Optional: increment on incompatible schema changes</td></tr>
 * </table>
 *
 * <h2>Topic Catalogue — Ad Analytics Platform</h2>
 * <pre>
 * {env}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session
 *   → Write path: ingestion-service produces CLICK/IMPRESSION/ADD_TO_CART …
 *   → Read  path: stream-processing-engine (Flink) consumes
 *   → Key  : tenantId:sessionId  (co-locates session events for stateful joins)
 *
 * {env}.internal.event.ads.clickstream.ad-interaction-failed-by-tenant-id
 *   → Dead-letter queue for rejected/invalid events
 *   → Consumed by SRE replay tooling / alerting
 *   → Key  : tenantId
 *
 * {env}.internal.event.ads.attribution.ad-interaction-enriched-by-campaign
 *   → Enriched + attributed events (CLICK_TO_BASKET synthetics included)
 *   → Produced by: Flink AttributionJoinFunction / Spring KafkaAggregateSink
 *   → Consumed by: Apache Pinot RealtimeTable + insights-query-service AggregateConsumer
 *   → Key  : tenantId:campaignId  (co-locates campaign data on same partition for Pinot)
 * </pre>
 *
 * <h2>Consumer-Group Catalogue</h2>
 * <pre>
 * {env}.ads.stream-processing-engine   — Flink job / Spring-Kafka fallback consumer
 * {env}.ads.insights-query-service     — serving-layer aggregate consumer
 * </pre>
 *
 * <p><strong>Runtime resolution:</strong> actual topic names and consumer-group names are
 * <em>never</em> hardcoded in application code.  They are expressed as Spring property
 * placeholders ({@code ${platform.kafka.topic.raw}} etc.) bound from
 * {@code application.yml}, with the {@code {env}} segment injected via the
 * {@code KAFKA_ENV} environment variable.  This file documents the convention and
 * provides the <em>template strings</em> used to build those properties.
 */
public final class KafkaTopics {

    private KafkaTopics() { }

    // =========================================================================
    // Topic name TEMPLATES (environment segment = "{env}" placeholder)
    // These are used as documentation anchors and for testing utilities.
    // Application code MUST use ${platform.kafka.topic.*} properties instead.
    // =========================================================================

    /**
     * Raw ad-interaction event stream.
     * <br>Visibility: {@code shared} · Type: {@code event}
     * <br>Key: {@code tenantId:sessionId}
     */
    public static final String TEMPLATE_RAW =
            "{env}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session";

    /**
     * Dead-letter queue for rejected or unprocessable events.
     * <br>Visibility: {@code internal} · Type: {@code event}
     * <br>Key: {@code tenantId}
     */
    public static final String TEMPLATE_DLQ =
            "{env}.internal.event.ads.clickstream.ad-interaction-failed-by-tenant-id";

    /**
     * Enriched and attributed events written by the stream engine.
     * Consumed by Apache Pinot (RealtimeTable) and the insights-query-service.
     * <br>Visibility: {@code internal} · Type: {@code event}
     * <br>Key: {@code tenantId:campaignId}
     */
    public static final String TEMPLATE_ENRICHED =
            "{env}.internal.event.ads.attribution.ad-interaction-enriched-by-campaign";

    /**
     * Reconciliation correction commands produced by the
     * {@code ReconciliationJob} when the daily Pinot-vs-Iceberg check detects
     * a count divergence beyond the configured threshold.
     *
     * <p>Downstream consumers (e.g. an SRE replay tool or the stream-processing-engine)
     * can listen to this topic and trigger selective event replay from Iceberg/S3.
     *
     * <br>Visibility: {@code internal} · Type: {@code command}
     * <br>Key: {@code tenantId:campaignId}
     */
    public static final String TEMPLATE_RECONCILIATION_CORRECTIONS =
            "{env}.internal.command.ads.reconciliation.counter-correction-by-campaign";

    // =========================================================================
    // Consumer-group TEMPLATES
    // =========================================================================

    /** Consumer group for the stream-processing-engine (Flink job or Spring-Kafka fallback). */
    public static final String TEMPLATE_GROUP_STREAM_ENGINE =
            "{env}.ads.stream-processing-engine";

    /** Consumer group for the insights-query-service serving layer. */
    public static final String TEMPLATE_GROUP_INSIGHTS_SERVING =
            "{env}.ads.insights-query-service";

    // =========================================================================
    // Legacy constants — kept for backward compatibility in unit tests.
    // Do NOT use these in production application code; use the Spring
    // ${platform.kafka.topic.*} properties instead.
    // =========================================================================

    /** @deprecated Use {@code ${platform.kafka.topic.raw}} Spring property. */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String RAW = "events.shopping.raw";

    /** @deprecated Use {@code ${platform.kafka.topic.dlq}} Spring property. */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String DLQ = "events.shopping.dlq";

    /**
     * @deprecated Consolidated into {@code ENRICHED}. Use
     *             {@code ${platform.kafka.topic.enriched}} Spring property.
     */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String AGGREGATES = "events.shopping.aggregates";

    /**
     * @deprecated Consolidated into {@code ENRICHED}. Use
     *             {@code ${platform.kafka.topic.enriched}} Spring property.
     */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String PINOT_ENRICHED = "events.shopping.enriched";

    /** @deprecated Use {@code ${platform.kafka.consumer-group.stream-engine}} property. */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String GROUP_STREAM_ENGINE = "group_stream_engine";

    /** @deprecated Use {@code ${platform.kafka.consumer-group.insights-serving}} property. */
    @Deprecated(since = "2.0", forRemoval = true)
    public static final String GROUP_INSIGHTS_SERVING = "group_insights_serving";

    // =========================================================================
    // Utility — build a concrete topic name from a template for a given env.
    // =========================================================================

    /**
     * Resolve a topic-name template by substituting the {@code {env}} placeholder.
     *
     * @param template one of the {@code TEMPLATE_*} constants
     * @param env      logical environment (e.g. {@code "prod"})
     * @return the concrete topic name
     */
    public static String resolve(String template, String env) {
        return template.replace("{env}", env);
    }
}
