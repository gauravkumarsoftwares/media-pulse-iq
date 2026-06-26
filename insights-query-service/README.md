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
│                        insights-query-service                            │
│                                                                         │
│  PasetoAuthenticationFilter (@Order 0)                                  │
│    └─ verifies v4.local, enforces read:ads scope                        │
│    └─ exposes PasetoClaims (allowed_campaigns) as request attribute     │
│                                                                         │
│  AdInsightsController ──► InsightsService                               │
│    GET /api/v1/campaigns/{id}/clicks           │                        │
│    GET /api/v1/campaigns/{id}/impressions      │                        │
│    GET /api/v1/campaigns/{id}/click-to-basket  │                        │
│                                                ▼                        │
│                                   TierRoutingEngine                     │
│                                        │                                │
│                              ┌─────────┼──────────┐                    │
│                        <48h  ▼   <30d  ▼   >30d   ▼                    │
│                           Redis    Pinot      Trino                     │
│                        (hot)    (warm)      (cold/Iceberg)              │
│                                                                         │
│  ReconciliationController ──► ReconciliationQueryService                │
│    GET /api/v1/reconciliation/status           │                        │
│    GET /api/v1/reconciliation/reports/{window} │                        │
│    POST /api/v1/reconciliation/runs/{window}   ▼                        │
│                                          ReconciliationJob              │
│                                     (scheduled: hourly + daily)         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Responsibilities

| Concern | Implementation |
|:--------|:---------------|
| **Authentication** | `PasetoAuthenticationFilter` — in-service `v4.local` verification, `read:ads` scope enforcement |
| **Per-campaign authz** | `InsightsServiceImpl` — enforces `allowed_campaigns` claim; returns 403 on violation |
| **Tier routing** | `TierRoutingEngine` — routes query to Redis / Pinot / Trino based on the query window start |
| **Hot tier** | `RedisInsightsStore` — `HGET campaign:{tenantId}:{campaignId}` counters (<48 h) |
| **Warm tier** | `PinotRestClient` — Apache Pinot REST query API (real-time table, <30 d) |
| **Cold tier** | `TrinoIcebergClient` — Trino JDBC against the Iceberg/S3 archive (>30 d) |
| **Reconciliation** | `ReconciliationJob` — scheduled hourly (Redis vs Pinot) and daily (Pinot vs Iceberg) |
| **Observability** | `QueryMetrics` + `ReconciliationMetrics` — Micrometer, Prometheus |

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
│   ├── KafkaConfig.java             # AggregateConsumer Kafka consumer factory
│   ├── PinotProperties.java         # Binds platform.pinot.*
│   └── PlatformInfraConfig.java     # Redis + Pinot bean wiring
│
├── consumer/
│   └── AggregateConsumer.java       # Spring-Kafka consumer: updates Redis counters from enriched topic
│
├── controller/
│   └── AdInsightsController.java    # GET /api/v1/campaigns/{id}/{metric} — thin controller
│
├── dto/
│   ├── CampaignMetricResponse.java  # Typed response: tenantId, campaignId, metric, series[], total, source
│   ├── TimeSeriesPoint.java         # {timestamp, value} bucket
│   ├── ReconciliationDetailDto.java # Full report with results[]
│   ├── ReconciliationResultDto.java # Per-campaign comparison row
│   ├── ReconciliationStatusEntry.java # Lightweight status summary per window
│   └── ReconciliationSummaryDto.java  # Compact report for paginated list
│
├── observability/
│   └── QueryMetrics.java            # Micrometer: query_latency_ms, query_tier_total, etc.
│
├── reconciliation/
│   ├── CampaignKey.java             # Value record (tenantId, campaignId, eventType)
│   ├── CampaignMetricCount.java     # CampaignKey + count from a data store
│   ├── ReconciliationController.java # GET/POST /api/v1/reconciliation/** — thin controller
│   ├── ReconciliationJob.java       # @Scheduled hourly + daily reconciliation logic
│   ├── ReconciliationMetrics.java   # Micrometer: reconciliation_discrepancies_total, etc.
│   ├── ReconciliationProperties.java # Binds platform.reconciliation.*
│   ├── ReconciliationReport.java    # Immutable run report (Builder pattern)
│   ├── ReconciliationResult.java    # Per-campaign comparison result (record)
│   ├── ReconciliationStore.java     # In-memory ring-buffer, 48 reports per window
│   └── ReconciliationWindow.java    # Enum: HOURLY (2h), DAILY (24h)
│
├── router/
│   ├── QueryTier.java               # Enum: REDIS_HOT, PINOT_WARM, TRINO_COLD
│   └── TierRoutingEngine.java       # Resolves tier by fromInstant age
│
├── security/
│   ├── PasetoAuthenticationFilter.java # @Order(0): v4.local verification, read:ads scope
│   ├── PasetoProperties.java           # Binds platform.security.paseto.*
│   └── PasetoSecurityConfig.java       # Builds PasetoVerifier bean; fail-closed guard
│
├── service/
│   ├── InsightsService.java             # Interface: getMetrics(...)
│   ├── InsightsServiceImpl.java         # Input validation, authz, tier routing, query, DTO mapping
│   ├── QueryService.java                # Interface: getCampaignCount + getTimeSeries
│   ├── ReconciliationQueryService.java  # Interface: getStatus, listReports, getLatestReport, triggerRun
│   ├── ReconciliationQueryServiceImpl.java # Wraps ReconciliationJob + ReconciliationStore; maps to DTOs
│   └── TieredInsightsEngine.java        # QueryService impl: dispatches to Redis/Pinot/Trino by tier
│
└── store/
    ├── PinotRestClient.java     # Apache Pinot REST API client (warm tier)
    ├── RedisInsightsStore.java  # Redis HGET/HINCRBY for hot-tier counters
    ├── StarTreeStore.java       # Pinot StarTree pre-aggregation (fast warm-tier path)
    └── TrinoIcebergClient.java  # Trino JDBC client for cold Iceberg queries
```

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
    "runTime":      "2026-06-26T05:05:00Z",
    "windowStart":  "2026-06-26T03:05:00Z",
    "windowEnd":    "2026-06-26T05:05:00Z",
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
  "timestamp":   "2026-06-26T08:00:00.000Z",
  "status":      400,
  "error":       "Bad Request",
  "message":     "Invalid grain. Allowed: minute, hour, day.",
  "path":        "/api/v1/campaigns/cmp1/clicks",
  "fieldErrors": []
}
```

---

## Tiered Query Routing

The `TierRoutingEngine` selects the serving tier based on the query window start time:

```
from == null  OR  from > (now - 48h)  →  REDIS_HOT    (sub-millisecond, HINCRBY counters)
from > (now - 30d)                    →  PINOT_WARM   (seconds, Pinot REST/StarTree)
from <= (now - 30d)                   →  TRINO_COLD   (seconds–minutes, Iceberg on S3)
```

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

The `source` field in `CampaignMetricResponse` tells the caller which tier served the query.

---

## Security Model

Same edge-validated PASETO model as the ingestion service (see `docs/AUTHENTICATION.md`):

1. Kong verifies the external `v4.public` token and mints an internal `v4.local` token.
2. `PasetoAuthenticationFilter` (this service) verifies the `v4.local` token, enforces the **`read:ads` scope**, and exposes the full `PasetoClaims` as a request attribute.
3. `InsightsServiceImpl` enforces the **`allowed_campaigns`** claim per campaign ID — returns **403** if the campaign is outside the token's allow-list.
4. The verified `tenantId` is injected as a mandatory predicate in every datastore query (Row-Level Security) — cross-tenant reads are structurally impossible.

---

## Reconciliation Sub-System

The reconciliation system detects and auto-corrects silent count drift across the three serving tiers.

### Hourly Job — Redis vs Pinot (last 2 hours)

- Runs at `cron: "0 5 * * * *"` (configurable via `platform.reconciliation.hourly-cron`).
- Queries Pinot for all campaign/metric counts in the last 2-hour window.
- Compares each against the corresponding Redis hot-counter.
- **Redis under-count** (Redis < Pinot): auto-patches the deficit via `HINCRBY` when `auto-correct-redis=true`.
- **Redis over-count** (Redis > Pinot): logs a warning only — possible in-flight Kafka lag; no auto-patch.

### Daily Job — Pinot vs Iceberg (previous calendar day)

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

**Auth scheme behaviour:**

| `auth-scheme` | `Authorization` header sent | When to use |
|:--------------|:---------------------------|:------------|
| `NONE` | — (none) | Local / open clusters |
| `BASIC` | `Basic <base64(user:pass)>` | Pinot with basic-auth enabled (`staging` / `prod`) |
| `TOKEN` | `Bearer <token>` | Pinot with JWT / PASETO token auth |

Credentials are **never committed** — they are injected from `PINOT_USERNAME`,
`PINOT_PASSWORD`, or `PINOT_AUTH_TOKEN` environment variables sourced from the
`app-secrets` Kubernetes Secret (see `deploy/config/app-secrets.example.env`).

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

Same properties as `ingestion-service` — see [ingestion-service README](../ingestion-service/README.md#security-model).

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
| **Typed DTOs (`CampaignMetricResponse`, `TimeSeriesPoint`)** | Replaces raw `Map<String, Object>` responses with compile-time-safe records; enables OpenAPI schema generation |
| **`InsightsService` interface** | Decouples controller from `QueryService`, `TierRoutingEngine`, and validation logic; each independently testable |
| **`ReconciliationQueryService` interface** | Decouples controller from `ReconciliationJob` + `ReconciliationStore`; all DTO mapping centralized here |
| **`PagedResponse<T>` envelope** | Consistent paginated response shape protects against unbounded list responses; clients can rely on `total` and `totalPages` |
| **`allowed_campaigns` in service layer** | Per-campaign authorization happens after tier routing is set up but before the query executes — ensures no data leaks even if the edge-level claim wasn't set |
| **In-memory `ReconciliationStore`** | Ring-buffer (48 reports/window) is sufficient for operational inspection; no external DB dependency; TTL is implicit via the fixed-size buffer |
| **Auto-patch only for Redis deficit** | Redis over-count is NOT auto-patched because it may reflect in-flight Kafka events not yet visible in Pinot — patching would reduce a valid count |

