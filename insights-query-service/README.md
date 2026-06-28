# insights-query-service

> **Role in the platform:** CQRS Read Path — the HTTP query service that serves campaign metric aggregates (clicks, impressions, click-to-basket) from a tiered data store (Redis hot cache → Apache Pinot → Trino/Iceberg), with an embedded reconciliation sub-system to detect and auto-correct count drift across tiers.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Package Structure](#package-structure)
- [API Reference](#api-reference)
  - [Campaign Insights API](#campaign-insights-api)
  - [Reconciliation API](#reconciliation-api)
  - [Error Envelope](#error-envelope)
- [Tiered Query Routing](#tiered-query-routing)
- [Security Model](#security-model)
- [Reconciliation Sub-System](#reconciliation-sub-system)
- [Configuration Reference](#configuration-reference)
- [Running Locally](#running-locally)
- [Building & Testing](#building--testing)
- [Key Design Decisions](#key-design-decisions)

---

## Architecture Overview

```
Client / SDK
    │  HTTPS + PASETO v4.public token
    ▼
Kong API Gateway (Edge)
    │  verifies Ed25519, mints v4.local, injects X-Internal-Token + X-Tenant-Context
    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        insights-query-service                           │
│                                                                         │
│  PasetoAuthenticationFilter (@Order 0)                                  │
│    └─ extends AbstractPasetoAuthenticationFilter (shared-security)      │
│    └─ verifies v4.local, enforces read:ads scope                        │
│    └─ exposes PasetoClaims (allowed_campaigns) as request attribute     │
│                                                                         │
│  AdInsightsController ──► InsightsService                               │
│    GET /api/v1/campaigns/{id}/clicks           │                        │
│    GET /api/v1/campaigns/{id}/impressions      │                        │
│    GET /api/v1/campaigns/{id}/click-to-basket  │                        │
│                                                ▼                        │
│                                   InsightsServiceImpl                   │
│                                   (validation + authz + tier routing)   │
│                                        │                                │
│                               TieredInsightsEngine (@TieredQuery AOP)   │
│                                        │                                │
│                               TierRoutingEngine                         │
│                              ┌─────────┼──────────┐                     │
│                        <48h  ▼   <30d  ▼   >30d   ▼                     │
│                  RedisCacheTier  PinotOlapTier  TrinoLakehouseTier      │
│                  TierHandler     TierHandler      TierHandler           │
│                                                                         │
│  ReconciliationController ──► ReconciliationQueryService                │
│    GET /api/v1/reconciliation/status           │                        │
│    GET /api/v1/reconciliation/reports/{window} │                        │
│    POST /api/v1/reconciliation/runs/{window}   ▼                        │
│                                   ReconciliationJob                     │
│                             (HourlyReconciliationStrategy +             │
│                              DailyReconciliationStrategy)               │
└─────────────────────────────────────────────────────────────────────────┘
```

### Responsibilities

| Concern | Implementation |
|:--------|:---------------|
| **Authentication** | `PasetoAuthenticationFilter` — thin subclass of `AbstractPasetoAuthenticationFilter`; declares `read:ads` scope |
| **Per-campaign authz** | `InsightsServiceImpl` — enforces `allowed_campaigns` claim; returns 403 on violation |
| **Input validation** | `InsightsRequestValidator` — `campaignId`/`placement` format, `grain` whitelist, window sanity checks |
| **Tier routing** | `TierRoutingEngine` — selects `QueryTier` based on configurable window boundaries (`TierRoutingProperties`) |
| **Hot tier** | `RedisCacheTierHandler` — `HGET`/`HGETALL` on `RedisKeySchema` keys (<48 h) |
| **Warm tier** | `PinotOlapTierHandler` + `StarTreeFallbackResolver` — Apache Pinot native SDK; falls back to in-memory StarTree locally |
| **Cold tier** | `TrinoLakehouseTierHandler` — Trino JDBC against the Iceberg/S3 archive (>30 d) |
| **Metrics (AOP)** | `QueryMetricsAspect` + `TierQueryContext` + `@TieredQuery` — wraps `TieredInsightsEngine` methods with Micrometer timer/counter via AOP |
| **Reconciliation** | `ReconciliationJob` — dispatches to `HourlyReconciliationStrategy` + `DailyReconciliationStrategy` |
| **Access logging** | `RequestLoggingFilterConfig` — Spring `CommonsRequestLoggingFilter` (DEBUG-gated) |

---

## Package Structure

```
com.java.query/
├── common/
│   ├── ApiErrorResponse.java        # Error envelope {timestamp, status, error, message, path, fieldErrors}
│   ├── ApiException.java            # Runtime exception with explicit HttpStatus
│   ├── GlobalExceptionHandler.java  # @RestControllerAdvice: 422, 400, ApiException, 500
│   └── PagedResponse.java           # Generic paginated envelope {items, total, page, pageSize, totalPages}
│
├── config/
│   ├── KafkaConfig.java             # AggregateConsumer factory; manual ack, DefaultErrorHandler, observation-enabled (OB-1)
│   ├── PinotProperties.java         # Binds platform.pinot.*
│   ├── PlatformInfraConfig.java     # Redis + Pinot bean wiring
│   ├── QueryProperties.java         # Binds platform.query.* (e.g. maxBuckets)
│   ├── RequestLoggingFilterConfig.java # CommonsRequestLoggingFilter: full URI, headers, body up to 4 KB
│   └── TrinoProperties.java         # Binds platform.trino.* (JDBC URL, pool, TLS, auth)
│
├── consumer/
│   └── AggregateConsumer.java       # ConsumerRecord listener; extracts X-Request-Id header → MDC (OB-2)
│
├── filter/
│   └── MdcFilter.java               # @Order(1): injects requestId/tenantId into SLF4J MDC for all HTTP requests (OB-2)
│
├── controller/
│   └── AdInsightsController.java    # GET /api/v1/campaigns/{id}/{metric} — thin controller
│
├── dto/
│   ├── CampaignMetricResponse.java  # Typed response: tenantId, campaignId, metric, series[], total, source
│   ├── TimeSeriesPoint.java         # {timestamp, value} bucket
│   ├── ReconciliationDetailDto.java # Full report with results[]
│   ├── ReconciliationResultDto.java # Per-campaign comparison row
│   ├── ReconciliationRunSummary.java  # Lightweight status summary per window
│   └── ReconciliationSummaryDto.java  # Compact report for paginated list
│
├── handler/
│   ├── TierQueryHandler.java         # Strategy interface: resolveCount + resolveTimeSeries per tier
│   ├── RedisCacheTierHandler.java    # Hot tier (<48 h): Redis HGET/HGETALL via RedisKeySchema
│   ├── PinotOlapTierHandler.java     # Warm tier (<30 d): Apache Pinot native SDK
│   ├── StarTreeFallbackResolver.java # In-memory StarTree fallback when Pinot is disabled (local/dev)
│   └── TrinoLakehouseTierHandler.java # Cold tier (>30 d): Trino JDBC → Iceberg/S3
│
├── observability/
│   ├── QueryMetrics.java            # Micrometer: query_latency_ms, query_tier_total, etc.
│   ├── QueryMetricsAspect.java      # @Around @TieredQuery: start/stop latency sample, reads TierQueryContext
│   ├── TierQueryContext.java        # ThreadLocal carrier: tier label, tenantId, metricType for AOP
│   └── TieredQuery.java             # Method annotation: marks TieredInsightsEngine entry points for AOP
│
├── reconciliation/
│   ├── CampaignKey.java             # Value record (tenantId, campaignId, eventType)
│   ├── CampaignMetricCount.java     # CampaignKey + count from a data store
│   ├── DailyReconciliationStrategy.java  # ReconciliationStrategy impl: Pinot vs Iceberg (previous day)
│   ├── HourlyReconciliationStrategy.java # ReconciliationStrategy impl: Redis vs Pinot (last 2 h)
│   ├── ReconciliationController.java # GET/POST /api/v1/reconciliation/** — thin controller
│   ├── ReconciliationJob.java       # @Scheduled scheduler: dispatches to ReconciliationStrategy impls
│   ├── ReconciliationMetrics.java   # Micrometer: reconciliation_discrepancies_total, etc.
│   ├── ReconciliationProperties.java # Binds platform.reconciliation.*
│   ├── ReconciliationReport.java    # Immutable run report (Builder pattern)
│   ├── ReconciliationResult.java    # Per-campaign comparison result (record)
│   ├── ReconciliationStore.java     # In-memory ring-buffer, 48 reports per window
│   ├── ReconciliationStrategy.java  # Strategy interface: supportedWindow() + execute() + buildReport()
│   └── ReconciliationWindow.java    # Enum: HOURLY (2h), DAILY (24h)
│
├── router/
│   ├── QueryTier.java               # Enum: REDIS_HOT, PINOT_WARM, TRINO_COLD
│   ├── TierRoutingEngine.java       # Resolves QueryTier by fromInstant age vs TierRoutingProperties
│   └── TierRoutingProperties.java   # Binds platform.query.routing.* (hotWindowHours, warmWindowDays)
│
├── security/
│   ├── PasetoAuthenticationFilter.java # @Order(0): extends AbstractPasetoAuthenticationFilter; declares read:ads scope
│   └── PasetoSecurityConfig.java       # (auto-configured from shared-security; no local config needed)
│
├── service/
│   ├── InsightsService.java             # Interface: getMetrics(...)
│   ├── InsightsServiceImpl.java         # Input validation, authz, tier routing, query, DTO mapping
│   ├── InsightsRequestValidator.java    # Validates campaignId/placement format, grain whitelist, window bounds
│   ├── QueryService.java                # Interface: getCampaignCount + getTimeSeries
│   ├── ReconciliationQueryService.java  # Interface: getStatus, listReports, getLatestReport, triggerRun
│   ├── ReconciliationQueryServiceImpl.java # Wraps ReconciliationJob + ReconciliationStore; maps to DTOs
│   ├── TieredInsightsEngine.java        # QueryService impl: dispatches to TierQueryHandler by resolved tier
│   └── TimeSeriesBucketUtils.java       # Builds grain-aligned time buckets (capped by QueryProperties.maxBuckets)
│
└── store/
    ├── PinotRestClient.java     # Apache Pinot native SDK client (warm tier)
    ├── RedisInsightsStore.java  # Redis HGET/HGETALL using RedisKeySchema (hot tier)
    ├── StarTreeStore.java       # In-memory Pinot StarTree pre-aggregation (local fallback)
    └── TrinoIcebergClient.java  # Trino JDBC client for cold Iceberg queries (HikariCP pool)
```

> `PasetoProperties` and `PasetoSecurityConfig` are **auto-configured** from the `shared-security` dependency. Services only declare the thin `PasetoAuthenticationFilter` subclass.

---

## API Reference

Base URL: `http://localhost:8083`  
Swagger UI: `http://localhost:8083/swagger-ui.html`  
OpenAPI spec: `http://localhost:8083/v3/api-docs`

### Campaign Insights API

#### GET `/api/v1/campaigns/{campaignId}/clicks`
#### GET `/api/v1/campaigns/{campaignId}/impressions`
#### GET `/api/v1/campaigns/{campaignId}/click-to-basket`

Returns a time-bucketed metric series and scalar total for the given campaign.

**Headers**

| Header | Required | Description |
|:-------|:---------|:------------|
| `X-Tenant-Context` | Yes | Verified tenant identifier |

**Path Parameters**

| Parameter | Description |
|:----------|:------------|
| `campaignId` | Campaign identifier (allow-list: `[A-Za-z0-9_.:-]{1,128}`) |

**Query Parameters**

| Parameter | Default | Description |
|:----------|:--------|:------------|
| `from` | _(hot window)_ | ISO-8601 window start (e.g. `2026-06-25T00:00:00Z`) |
| `to` | now | ISO-8601 window end |
| `grain` | `hour` | Bucket size: `minute`, `hour`, `day` |
| `placement` | _(all)_ | Optional placement filter (`[A-Za-z0-9_.:-]{1,128}`) |

**Success Response — `200 OK`**

```json
{
  "tenantId":        "walmart_us",
  "campaignId":      "cmp_spring_99a",
  "metric":          "click",
  "from":            "2026-06-25T00:00:00Z",
  "to":              "2026-06-26T00:00:00Z",
  "grain":           "hour",
  "placement":       null,
  "series": [
    { "timestamp": "2026-06-25T00:00:00Z", "value": 1420 },
    { "timestamp": "2026-06-25T01:00:00Z", "value": 1893 }
  ],
  "total":           42156,
  "dataFreshnessMs": 12,
  "source":          "redis-hot"
}
```

| Field | Description |
|:------|:------------|
| `source` | Which tier served this query: `redis-hot`, `pinot-warm`, or `trino-cold` |
| `dataFreshnessMs` | Approximate query execution time (tier-dependent data age) |

**Error Responses**

| Status | Condition |
|:-------|:----------|
| `401 Unauthorized` | Missing `X-Tenant-Context` header |
| `403 Forbidden` | Token missing `read:ads` scope, or `campaignId` not in `allowed_campaigns` |
| `400 Bad Request` | Invalid `campaignId`/`placement` format or unrecognized `grain` |

---

### Reconciliation API

#### GET `/api/v1/reconciliation/status`

Returns the latest run summary for each window type (`hourly`, `daily`).

**Response — `200 OK`**

```json
{
  "hourly": {
    "runId":        "9b3f12a0-...",
    "status":       "OK",
    "runTime":      "2026-06-27T05:05:00Z",
    "windowStart":  "2026-06-27T03:05:00Z",
    "windowEnd":    "2026-06-27T05:05:00Z",
    "campaigns":    1247,
    "discrepancies": 0,
    "autoPatched":  0,
    "elapsedMs":    340
  },
  "daily": {
    "status": "NO_RUN_YET"
  }
}
```

---

#### GET `/api/v1/reconciliation/reports/{window}`

Paginated list of report summaries, newest first.

| Path Param | Values |
|:-----------|:-------|
| `window` | `hourly` or `daily` |

| Query Param | Default | Description |
|:------------|:--------|:------------|
| `limit` | `10` | Max reports (capped at 48) |

**Response — `200 OK`** (`PagedResponse<ReconciliationSummaryDto>`)

```json
{
  "items": [ { "runId": "...", "status": "OK", ... } ],
  "total": 1,
  "page": 0,
  "pageSize": 1,
  "totalPages": 1
}
```

---

#### GET `/api/v1/reconciliation/reports/{window}/latest`

Returns the most recent **full** report including per-campaign comparison results.

**Response — `200 OK`**

```json
{
  "runId":     "9b3f12a0-...",
  "status":    "DISCREPANCIES_FOUND",
  "campaigns": 1247,
  "results": [
    {
      "tenantId":       "walmart_us",
      "campaignId":     "cmp_spring_99a",
      "eventType":      "CLICK",
      "referenceStore": "pinot",
      "referenceCount": 42000,
      "observedStore":  "redis",
      "observedCount":  41997,
      "delta":          -3,
      "discrepancyPct": "0.0071",
      "autoPatched":    true
    }
  ]
}
```

---

#### POST `/api/v1/reconciliation/runs/{window}`

Triggers an on-demand reconciliation run synchronously. Returns the completed full report.

> ⚠️ **Production note:** Protect this endpoint with an admin role or restrict to the internal network. Do not expose via public ingress.

**Response — `200 OK`**: same shape as `GET .../latest`.

---

### Error Envelope

```json
{
  "timestamp":   "2026-06-27T08:00:00.000Z",
  "status":      400,
  "error":       "Bad Request",
  "message":     "Invalid grain. Allowed: minute, hour, day.",
  "path":        "/api/v1/campaigns/cmp1/clicks",
  "fieldErrors": []
}
```

---

## Tiered Query Routing

The `TierRoutingEngine` selects the serving tier based on the query window start time, resolved against configurable boundaries in `TierRoutingProperties`:

```
from == null  OR  from > (now - hotWindowHours)   →  REDIS_HOT    (sub-millisecond, HINCRBY counters)
from > (now - warmWindowDays)                      →  PINOT_WARM   (seconds, Pinot REST/StarTree)
from <= (now - warmWindowDays)                     →  TRINO_COLD   (seconds–minutes, Iceberg on S3)
```

Default boundaries: `hotWindowHours=48`, `warmWindowDays=30`.

```
┌─────────────────────────────────────────────────────────────────────┐
│  Timeline (newest → oldest)                                         │
│                                                                     │
│   now ◄─── 48 h ──── REDIS HOT ─── 30 d ─── PINOT WARM ─ TRINO ──►│
│                                                                     │
│   Redis:  HINCRBY increment on ingest, TTL 48 h                    │
│   Pinot:  real-time table, ingests from Kafka enriched topic       │
│   Trino:  Iceberg on S3, Flink IcebergS3Sink, batch/archival       │
└─────────────────────────────────────────────────────────────────────┘
```

Each tier is encapsulated by a `TierQueryHandler` implementation:

| Tier | Handler | Store |
|:-----|:--------|:------|
| `REDIS_HOT` | `RedisCacheTierHandler` | Redis hash keys via `RedisKeySchema` |
| `PINOT_WARM` | `PinotOlapTierHandler` | Apache Pinot native Java SDK; `StarTreeFallbackResolver` when Pinot is disabled |
| `TRINO_COLD` | `TrinoLakehouseTierHandler` | Trino JDBC → Iceberg/S3 (HikariCP pool) |

The `source` field in `CampaignMetricResponse` tells the caller which tier served the query.

---

## Security Model

Same edge-validated PASETO model as the ingestion service (see `docs/AUTHENTICATION.md`):

1. Kong verifies the external `v4.public` token and mints an internal `v4.local` token.
2. `PasetoAuthenticationFilter` (this service, extends `AbstractPasetoAuthenticationFilter`) verifies the `v4.local` token, enforces the **`read:ads` scope**, and exposes the full `PasetoClaims` as a request attribute.
3. `InsightsServiceImpl` enforces the **`allowed_campaigns`** claim per campaign ID — returns **403** if the campaign is outside the token's allow-list.
4. The verified `tenantId` is injected as a mandatory predicate in every datastore query (Row-Level Security) — cross-tenant reads are structurally impossible.

`PasetoAuthenticationFilter` is a thin subclass — it declares only `requiredScope() → "read:ads"`. All verification, audit logging, and `TenantOverrideRequest` header rewriting are inherited from `AbstractPasetoAuthenticationFilter` in `shared-security`.

---

## Reconciliation Sub-System

The reconciliation system detects and auto-corrects silent count drift across the three serving tiers using a **Strategy pattern** — `ReconciliationJob` is a thin scheduler that dispatches to the correct `ReconciliationStrategy` implementation.

### HourlyReconciliationStrategy — Redis vs Pinot (last 2 hours)

- Runs at `cron: "0 5 * * * *"` (configurable via `platform.reconciliation.hourly-cron`).
- Queries Pinot for all campaign/metric counts in the last 2-hour window.
- Compares each against the corresponding Redis hot-counter.
- **Redis under-count** (Redis < Pinot): auto-patches the deficit via `HINCRBY` when `auto-correct-redis=true`.
- **Redis over-count** (Redis > Pinot): logs a warning only — possible in-flight Kafka lag; no auto-patch.

### DailyReconciliationStrategy — Pinot vs Iceberg (previous calendar day)

- Runs at `cron: "0 15 1 * * *"` (configurable via `platform.reconciliation.daily-cron`).
- Compares Pinot counts to the authoritative Iceberg/Trino source for the previous UTC day.
- Discrepancies beyond `discrepancy-threshold-pct` are logged, metered, and stored for REST inspection.

### Source-of-Truth Hierarchy

```
Iceberg (S3 Parquet) > Pinot (OLAP warm) > Redis (hot cache)
```

---

## Configuration Reference

### Server

| Property | Default | Description |
|:---------|:--------|:------------|
| `server.port` | `8083` | HTTP port |
| `management.server.port` | `9090` | Actuator port |

### Kafka (AggregateConsumer)

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.kafka.topic.enriched` | _(derived)_ | Enriched events input topic consumed by `AggregateConsumer` |
| `platform.kafka.consumer-group.insights-serving` | _(derived)_ | Consumer group ID |
| `spring.kafka.consumer.enable-auto-commit` | `false` | Manual offset commit — offset advanced only after `onMessage()` succeeds |
| `spring.kafka.listener.ack-mode` | `MANUAL_IMMEDIATE` | Commit mode enforced in `KafkaConfig` |
| `spring.kafka.listener.concurrency` | `3` (local) / `64` (prod) | Consumer thread count. Set via `KAFKA_LISTENER_CONCURRENCY` env var. Each thread owns whole partitions — ordering preserved |
| `spring.kafka.consumer.max-poll-records` | `500` | Records fetched per poll cycle |
| `spring.kafka.consumer.properties.fetch.min.bytes` | `1048576` | Broker waits until 1 MB available (reduces round-trips) |
| `spring.kafka.consumer.properties.fetch.max.wait.ms` | `500` | Max broker wait even if `fetch.min.bytes` not met |
| `spring.kafka.consumer.properties.max.poll.interval.ms` | `300000` | Must exceed time to process `max-poll-records` records |
| `spring.kafka.consumer.properties.session.timeout.ms` | `45000` | Broker detection timeout for dead consumers |
| `spring.kafka.consumer.properties.heartbeat.interval.ms` | `15000` | Heartbeat frequency (< session.timeout.ms / 3) |

**Consumer error handling:** `DefaultErrorHandler` with `FixedBackOff(1 s, 3 retries)`. After all retries fail the record is logged and skipped — `AggregateConsumer` is a read-side projection; the source of truth lives in Pinot/Iceberg, so no DLQ is required.

### Observability (OB-1 / OB-2 / OB-5)

| Property | Default | Description |
|:---------|:--------|:------------|
| `management.tracing.sampling.probability` | `1.0` (local) / `0.1` (prod) | Fraction of traces exported to OTEL Collector |
| `management.otlp.tracing.endpoint` | `http://otel-collector:4318/v1/traces` | OTLP HTTP endpoint — Jaeger locally, Grafana Tempo in prod |

**MDC Correlation (OB-2):** `MdcFilter` (`@Order(1)`) injects `requestId` (from `X-Request-Id` header, or a fresh UUID if absent) and `tenantId` into SLF4J MDC for every HTTP request. The correlation ID is echoed back in the `X-Request-Id` response header. `AggregateConsumer` switches from `ShoppingEvent` to `ConsumerRecord<String, ShoppingEvent>` to extract the `X-Request-Id` Kafka header and restore MDC context during async event indexing — enabling end-to-end log correlation across HTTP → Kafka → consumer.

**Structured Logs (OB-5):** Spring `prod` profile activates `LogstashEncoder` JSON appender in `logback-spring.xml`. JSON log lines include `requestId`, `tenantId`, `service=insights-query-service`, and `env=prod` fields for Fluent Bit → Loki ingestion and Grafana log correlation queries.

### Pinot

The service uses the **native Apache Pinot Java SDK** (`pinot-java-client`) with
`JsonAsyncHttpPinotClientTransportFactory` rather than a raw HTTP client. The
transport uses Netty-backed async I/O, broker failover, typed `ResultSet`
accessors, and injects auth headers at the connection level.

#### Connection properties

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.pinot.enabled` | `false` | Enable real Pinot queries (`false` = StarTree in-memory fallback) |
| `platform.pinot.broker` | `localhost:8099` | Broker `host:port` (without scheme) |
| `platform.pinot.scheme` | `http` | `http` or `https` (use `https` in staging/prod) |
| `platform.pinot.table` | `shopping_events` | Pinot realtime table name |
| `platform.pinot.connect-timeout-ms` | `3000` | TCP connect timeout |
| `platform.pinot.read-timeout-ms` | `10000` | Socket read timeout |

#### Broker authentication

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.pinot.auth-scheme` | `NONE` | `NONE` · `BASIC` · `TOKEN` |
| `platform.pinot.username` | `""` | Username for `BASIC` auth (from `PINOT_USERNAME` K8s Secret) |
| `platform.pinot.password` | `""` | Password for `BASIC` auth (from `PINOT_PASSWORD` K8s Secret) |
| `platform.pinot.auth-token` | `""` | Bearer token for `TOKEN` auth (from `PINOT_AUTH_TOKEN` K8s Secret) |

### Trino (Cold Tier)

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.trino.enabled` | `false` | Enable Trino cold-tier client |
| `platform.trino.jdbc-url` | `jdbc:trino://trino:8080/iceberg/ads` | Full Trino JDBC URL (catalog + schema must be in URL) |
| `platform.trino.user` | `insights-query-service` | Trino user (`X-Trino-User` header) — from `TRINO_USER` K8s Secret |
| `platform.trino.password` | `""` | Trino password — from `TRINO_PASSWORD` K8s Secret |
| `platform.trino.catalog` | `iceberg` | Iceberg catalog name |
| `platform.trino.schema` | `ads` | Iceberg schema name |
| `platform.trino.table` | `shopping_events` | Iceberg table name |
| `platform.trino.query-timeout-seconds` | `60` | Per-query timeout (cold scans can be slow) |
| `platform.trino.max-pool-size` | `10` | HikariCP max connections |
| `platform.trino.min-idle` | `2` | HikariCP min idle connections |
| `platform.trino.ssl-enabled` | `false` | Enable TLS for Trino JDBC (`true` in prod) |

### Tier Routing

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.query.routing.hot-window-hours` | `48` | Queries within this window are served by Redis hot tier |
| `platform.query.routing.warm-window-days` | `30` | Queries within this window (beyond hot) are served by Pinot warm tier |

### Query Engine

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.query.max-buckets` | `10000` | Maximum grain-aligned time buckets produced by `TimeSeriesBucketUtils` |

### Reconciliation

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.reconciliation.enabled` | `true` | Enable scheduled jobs |
| `platform.reconciliation.hourly-cron` | `0 5 * * * *` | Hourly job cron |
| `platform.reconciliation.daily-cron` | `0 15 1 * * *` | Daily job cron |
| `platform.reconciliation.auto-correct-redis` | `true` | Auto-patch Redis deficits |
| `platform.reconciliation.discrepancy-threshold-pct` | `0.01` | Alert threshold (1%) |
| `platform.reconciliation.max-campaigns-per-run` | `10000` | Pinot query row limit |

### Security (PASETO)

Auto-configured from `shared-security` — see [shared-security README](../shared-security/README.md#paseto-properties) for the full property reference.

---

## Running Locally

**Prerequisites:** Java 21, Docker (Kafka + Redis via Docker Compose).

```bash
# 1. Start dependencies
cd deploy
docker-compose up -d kafka redis

# 2. Run the service
cd insights-query-service
mvn spring-boot:run -Dspring-boot.run.profiles=local

# 3. Query clicks for a campaign
curl -s 'http://localhost:8083/api/v1/campaigns/cmp_spring_99a/clicks?grain=hour' \
  -H 'X-Tenant-Context: walmart_us'

# 4. Check reconciliation status
curl -s http://localhost:8083/api/v1/reconciliation/status

# 5. Trigger on-demand hourly reconciliation
curl -s -X POST http://localhost:8083/api/v1/reconciliation/runs/hourly

# Missing tenant header → 401
curl -s -i http://localhost:8083/api/v1/campaigns/cmp1/clicks
```

---

## Building & Testing

```bash
# Compile
mvn -pl insights-query-service compile

# Tests
mvn -pl insights-query-service test

# Fat JAR
mvn -pl insights-query-service package -DskipTests

# Docker
docker build -t insights-query-service:latest insights-query-service/
```

### Test Coverage

| Test Class | Type | Covers |
|:-----------|:-----|:-------|
| `AdInsightsControllerTest` | `@WebMvcTest` | 401 missing tenant, 200 typed DTO, 400 bad grain, 403 restricted campaign |
| `ReconciliationControllerTest` | `@WebMvcTest` | GET status, GET reports (paged), GET latest, POST run, 400 unknown window |
| `ReconciliationJobTest` | Unit | Hourly Redis patch, daily Pinot/Iceberg comparison |

---

## Key Design Decisions

| Decision | Rationale |
|:---------|:----------|
| **`TierQueryHandler` strategy interface** | Each tier (Redis, Pinot, Trino) is encapsulated behind the same `resolveCount` / `resolveTimeSeries` contract. `TieredInsightsEngine` dispatches to the correct handler by `QueryTier`; adding a new tier requires only a new handler — no changes to the engine (OCP) |
| **`TierRoutingProperties` (externalised thresholds)** | Tier boundaries (`hotWindowHours`, `warmWindowDays`) are Spring config properties, not hardcoded constants — changing them for staging vs prod requires only a config change, not a recompile |
| **`@TieredQuery` AOP + `TierQueryContext`** | Micrometer latency timers were removed from `TieredInsightsEngine` method bodies and replaced with an `@Around` aspect. The serving tier is resolved at runtime and passed to the aspect via a `ThreadLocal` (`TierQueryContext`), keeping method code free of metrics instrumentation |
| **`ReconciliationStrategy` strategy interface** | Splitting the monolithic `ReconciliationJob` into `HourlyReconciliationStrategy` + `DailyReconciliationStrategy` gives each implementation a single responsibility; `ReconciliationJob` is a pure scheduler with no reconciliation logic (SRP + OCP) |
| **`InsightsRequestValidator`** | Validation logic extracted from `InsightsServiceImpl` into a dedicated class — each class has one reason to change; validator is independently testable |
| **`TimeSeriesBucketUtils` + `maxBuckets` cap** | Bucket generation is centralised; `QueryProperties.maxBuckets` (default 10 000) prevents accidental OOM on very long windows with fine-grained buckets |
| **Typed DTOs (`CampaignMetricResponse`, `TimeSeriesPoint`)** | Replaces raw `Map<String, Object>` responses with compile-time-safe records; enables OpenAPI schema generation |
| **`ReconciliationQueryService` interface** | Decouples controller from `ReconciliationJob` + `ReconciliationStore`; all DTO mapping centralized here |
| **`PagedResponse<T>` envelope** | Consistent paginated response shape protects against unbounded list responses; clients can rely on `total` and `totalPages` |
| **`allowed_campaigns` in service layer** | Per-campaign authorization happens after tier routing is set up but before the query executes — ensures no data leaks even if the edge-level claim wasn't set |
| **In-memory `ReconciliationStore`** | Ring-buffer (48 reports/window) is sufficient for operational inspection; no external DB dependency; TTL is implicit via the fixed-size buffer |
| **Auto-patch only for Redis deficit** | Redis over-count is NOT auto-patched because it may reflect in-flight Kafka events not yet visible in Pinot — patching would reduce a valid count |
| **Manual ack on `AggregateConsumer`** | `MANUAL_IMMEDIATE` ack mode ensures the Kafka offset is committed only after `StarTreeStore.index()` succeeds. On failure `DefaultErrorHandler` retries 3 times; after exhaustion the record is logged and skipped — this service is a read-side projection backed by Pinot/Iceberg, so no DLQ is needed |
| **MDC correlation via `MdcFilter` (OB-2)** | `MdcFilter` (`@Order(1)`) injects `requestId` (from `X-Request-Id` header or auto-UUID) and `tenantId` into SLF4J MDC for every HTTP request. `AggregateConsumer` switches to `ConsumerRecord<String, ShoppingEvent>` to extract the `X-Request-Id` Kafka header and restore MDC context during event indexing — every log line carries the end-to-end correlation ID |
| **KafkaConfig observation-enabled (OB-1)** | `factory.getContainerProperties().setObservationEnabled(true)` enables Micrometer OTEL instrumentation. With `micrometer-tracing-bridge-otel` on the classpath, the W3C `traceparent` header is automatically extracted from each record, continuing the distributed trace from the ingestion-service |
| **Structured JSON logging (OB-5)** | `logback-spring.xml` activates `LogstashEncoder` in the `prod` profile. JSON lines include `requestId`, `tenantId`, `service`, `env` fields for Fluent Bit → Loki aggregation |
