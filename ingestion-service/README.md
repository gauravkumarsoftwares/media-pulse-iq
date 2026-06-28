# ingestion-service

> **Role in the platform:** CQRS Write Path — the public-facing HTTP gateway that accepts ad-interaction events from clients, enforces security and rate limiting, validates payloads, and publishes them to the Kafka raw topic for downstream stream processing.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Package Structure](#package-structure)
- [API Reference](#api-reference)
  - [POST /api/v1/events](#post-apiv1events)
  - [Error Envelope](#error-envelope)
- [Security Model](#security-model)
- [Rate Limiting](#rate-limiting)
- [Configuration Reference](#configuration-reference)
- [Running Locally](#running-locally)
- [Building & Testing](#building--testing)
- [Key Design Decisions](#key-design-decisions)

---

## Architecture Overview

```
Client / SDK
    │
    │  HTTPS + PASETO v4.public token (Authorization)
    ▼
Kong API Gateway (Edge)
    │  verifies Ed25519 signature, mints v4.local internal token
    │  injects X-Internal-Token + X-Tenant-Context
    ▼
┌──────────────────────────────────────────────────────────┐
│                   ingestion-service                       │
│                                                          │
│  PasetoAuthenticationFilter (@Order 0)                   │
│    └─ extends AbstractPasetoAuthenticationFilter         │
│    └─ verifies v4.local token (shared symmetric key)     │
│    └─ enforces write:events scope                        │
│    └─ rewrites X-Tenant-Context from verified claims     │
│                                                          │
│  TenantContextFilter (@Order 1)                          │
│    └─ defense-in-depth: rejects missing tenant header    │
│                                                          │
│  IngestController  ──►  IngestionService                 │
│                              │                           │
│                    ┌─────────┼──────────┐                │
│                    ▼         ▼          ▼                 │
│              RateLimiter SchemaValidator DlqProducer      │
│                                   │                      │
│                             EventProducer                 │
│                                   │                      │
└───────────────────────────────────┼──────────────────────┘
                                    │
                            Kafka Raw Topic
           {env}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session
```

### Responsibilities

| Concern | Implementation |
|:--------|:---------------|
| **Authentication** | `PasetoAuthenticationFilter` — extends `AbstractPasetoAuthenticationFilter` from `shared-security`; declares `write:events` scope |
| **Tenant isolation** | `TenantContextFilter` — defense-in-depth header presence check; populates MDC `requestId`/`tenantId`; controller re-check |
| **Rate limiting** | `TenantRateLimiter` — Redis sliding-window counter, per-tenant TPS limit |
| **Schema validation** | `SchemaValidator` — mandatory field presence + recognized `EventType` |
| **DLQ routing** | `DlqProducer` — invalid schema events AND permanent Kafka send failures routed to the dead-letter topic for SRE replay |
| **Event publish** | `EventProducer` — idempotent Avro-serialized `ShoppingEvent` to the Kafka raw topic; injects `X-Request-Id` Kafka header (OB-2); routes to DLQ on permanent broker failure |
| **DLQ replay** | `DlqReplayController` / `DlqReplayService` — admin REST endpoints to inspect and replay DLQ events (OB-4) |
| **Observability** | `IngestionMetrics` — Micrometer counters/timers; `DlqReplayMetrics` — DLQ replay counters; all exposed via Prometheus |
| **Distributed tracing** | Micrometer OTEL bridge — auto-instruments HTTP and Kafka sends with W3C `traceparent` header (OB-1) |
| **Access logging** | `RequestLoggingFilterConfig` — Spring `CommonsRequestLoggingFilter` (DEBUG-gated) |

---

## Package Structure

```
com.java.ingestion/
├── common/
│   ├── ApiErrorResponse.java      # Standardized error envelope {timestamp, status, error, message, path, fieldErrors}
│   ├── ApiException.java          # Runtime exception with explicit HttpStatus
│   └── GlobalExceptionHandler.java # @RestControllerAdvice: maps exceptions to ApiErrorResponse
│
├── config/
│   ├── KafkaProducerConfig.java   # KafkaTemplate + ProducerFactory wiring; observation-enabled=true (OB-1)
│   └── RequestLoggingFilterConfig.java # CommonsRequestLoggingFilter: full URI, headers, body up to 4 KB
│
├── controller/
│   └── IngestController.java      # POST /api/v1/events — thin controller, delegates to IngestionService
│
├── dlq/                           # OB-4: DLQ replay tooling
│   ├── DlqReplayController.java   # POST /api/v1/admin/dlq/replay, GET /api/v1/admin/dlq/stats
│   ├── DlqReplayService.java      # One-shot KafkaConsumer replay logic with tenantId/reason filtering
│   ├── DlqReplayMetrics.java      # ads_dlq_replayed_total, ads_dlq_replay_failed_total counters
│   ├── DlqReplayRequest.java      # Request DTO: tenantId, reason, maxEvents
│   └── DlqReplaySummary.java      # Response DTO: replayed, skipped, failed, total
│
├── dto/
│   ├── IngestEventRequest.java    # Input DTO with Jakarta Bean Validation (@NotBlank, @Size, @DecimalMin)
│   └── IngestEventResponse.java   # Acceptance confirmation {status, eventId, processedTimestamp, remainingQuota}
│
├── observability/
│   └── IngestionMetrics.java      # Micrometer: events_accepted_total, events_validation_failed_total, etc.
│
├── producer/
│   ├── DlqProducer.java           # Publishes rejected events to the DLQ Kafka topic with dlq-reason header
│   └── EventProducer.java         # Publishes validated events to raw topic; injects X-Request-Id header (OB-2)
│
├── ratelimit/
│   ├── RateLimitProperties.java   # Binds platform.rate-limit.* config
│   └── TenantRateLimiter.java     # Redis-backed sliding-window rate limiter (per tenant)
│
├── security/
│   ├── PasetoAuthenticationFilter.java # @Order(0): extends AbstractPasetoAuthenticationFilter; declares write:events scope
│   └── TenantContextFilter.java        # @Order(1): tenant header check + MDC requestId/tenantId injection (OB-2)
│
├── service/
│   ├── IngestionService.java      # Interface: ingest(IngestEventRequest, tenantId)
│   └── IngestionServiceImpl.java  # Impl: rate-limit → validate → DLQ-or-publish → metrics
│
├── validation/
│   └── SchemaValidator.java       # Validates eventId/userId/sessionId presence + EventType whitelist
│
└── IngestionApplication.java      # Spring Boot entry point
```

> `PasetoProperties` and `PasetoSecurityConfig` are **not** declared in this module — they are auto-configured from the `shared-security` dependency. The `PasetoVerifier` bean is constructed there based on `platform.security.paseto.*` properties.

---

## API Reference

Base URL: `http://localhost:8080`  
Swagger UI: `http://localhost:8080/swagger-ui.html`  
OpenAPI spec: `http://localhost:8080/v3/api-docs`

### POST /api/v1/events

Ingest a single ad-interaction event.

**Headers**

| Header | Required | Description |
|:-------|:---------|:------------|
| `X-Tenant-Context` | Yes | Verified tenant identifier (injected by the gateway) |
| `Content-Type` | Yes | `application/json` |

**Request Body** (`IngestEventRequest`)

```json
{
  "eventId":         "evt_abc123",
  "userId":          "usr_xyz",
  "sessionId":       "sess_001",
  "campaignId":      "cmp_spring_99a",
  "eventType":       "CLICK",
  "eventTimestampMs": 1750000000000,
  "cost":            0.05,
  "customTags": {
    "device": "mobile",
    "country": "US"
  }
}
```

| Field | Type | Constraints | Notes |
|:------|:-----|:------------|:------|
| `eventId` | `string` | `@NotBlank`, max 128 | Unique event identifier for deduplication |
| `userId` | `string` | `@NotBlank`, max 128 | — |
| `sessionId` | `string` | `@NotBlank`, max 128 | Used for sessionized attribution joins |
| `campaignId` | `string` | max 128, optional | Attribution target |
| `eventType` | `string` | `@NotBlank` | `CLICK`, `IMPRESSION`, `ADD_TO_CART`, `PURCHASE`, `PRODUCT_VIEW`, `CLICK_TO_BASKET` |
| `eventTimestampMs` | `long` | optional | Defaults to server time if 0 |
| `cost` | `double` | `@DecimalMin(0.0)` | Ad cost in USD |
| `customTags` | `map` | optional | Arbitrary key-value metadata |

**Success Response — `202 Accepted`**

```json
{
  "status":             "ACCEPTED",
  "eventId":            "evt_abc123",
  "processedTimestamp": "2026-06-27T08:00:00Z",
  "remainingQuota":     498
}
```

**Error Responses**

| Status | Condition |
|:-------|:----------|
| `401 Unauthorized` | Missing `X-Tenant-Context` header |
| `403 Forbidden` | Token missing `write:events` scope (when PASETO enabled) |
| `400 Bad Request` | Unrecognized `eventType` (sent to DLQ) |
| `422 Unprocessable Entity` | Jakarta Bean Validation failure (blank `eventId`, etc.) |
| `429 Too Many Requests` | Per-tenant rate limit exceeded |

### Error Envelope

All non-2xx responses return a consistent JSON envelope:

```json
{
  "timestamp":   "2026-06-27T08:00:00.000Z",
  "status":      422,
  "error":       "Unprocessable Entity",
  "message":     "Request validation failed",
  "path":        "/api/v1/events",
  "fieldErrors": [
    { "field": "eventId", "message": "eventId is required" }
  ]
}
```

`fieldErrors` is only populated for `422` responses; it is an empty array otherwise.

---

## Security Model

Authentication follows the **edge-validated PASETO, header-propagated trust** model (see `docs/AUTHENTICATION.md`).

```
External v4.public token → Kong verifies → mints v4.local internal token
→ X-Internal-Token forwarded to this service
→ PasetoAuthenticationFilter (extends AbstractPasetoAuthenticationFilter from shared-security)
    → verifies v4.local with shared key
    → enforces write:events scope
    → rewrites X-Tenant-Context from verified claims (TenantOverrideRequest)
→ TenantContextFilter: defense-in-depth header presence check
```

`PasetoAuthenticationFilter` is a thin subclass — it declares only `requiredScope() → "write:events"`. All verification logic, audit logging, and tenant header rewriting are inherited from `AbstractPasetoAuthenticationFilter` in `shared-security`.

**In local dev** (`spring.profiles.active=local`): `platform.security.paseto.enabled=false` — the filter is bypassed and the service trusts the gateway-injected `X-Tenant-Context` header directly.

**In staging/prod**: `mode=local` + `local-key=${PASETO_LOCAL_KEY}` — the service verifies the `v4.local` symmetric token in-process. The service **refuses to start** if `PASETO_LOCAL_KEY` is missing (fail-closed guard in `PasetoSecurityConfig`).

---

## Rate Limiting

Per-tenant sliding-window counter backed by Redis:

```yaml
platform:
  rate-limit:
    enforced: true
    default-events-per-second: 500
    window-seconds: 1
```

When a tenant exceeds their limit, the service returns `429 Too Many Requests` with:
- `Retry-After: 1` header
- `X-RateLimit-Remaining: 0` header
- Response: `{"error": "Rate limit exceeded. Reduce request frequency.", "tenantId": "..."}`

In local dev, `enforced: false` runs the limiter in warn-only mode (no requests blocked).

---

## Configuration Reference

All properties are in `src/main/resources/application[-profile].yml`.

### Core

| Property | Default | Description |
|:---------|:--------|:------------|
| `server.port` | `8080` | HTTP server port |
| `management.server.port` | `9090` | Actuator/metrics port (internal cluster only) |

### Kafka

| Property | Default | Description |
|:---------|:--------|:------------|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Kafka broker(s) |
| `spring.kafka.producer.acks` | `all` | All in-sync replicas must ack before send completes |
| `spring.kafka.producer.retries` | `2147483647` | Effectively infinite; bounded by `delivery.timeout.ms` |
| `spring.kafka.producer.properties.enable.idempotence` | `true` | Prevents broker-side duplicate writes on retry |
| `spring.kafka.producer.properties.max.in.flight.requests.per.connection` | `5` | Max safe value with idempotence enabled |
| `spring.kafka.producer.properties.delivery.timeout.ms` | `120000` | Total retry window (2 min) before DLQ fallback |
| `spring.kafka.producer.properties.request.timeout.ms` | `30000` | Per-attempt broker timeout |
| `spring.kafka.producer.properties.linger.ms` | `10` | Batch accumulation window (10 ms) for higher throughput |
| `spring.kafka.producer.properties.batch.size` | `65536` | Per-partition batch size (64 KB) |
| `spring.kafka.producer.properties.buffer.memory` | `67108864` | Total producer buffer before back-pressure (64 MB) |
| `spring.kafka.producer.properties.compression.type` | `lz4` | LZ4 compression (~50–60% size reduction on Avro payloads) |
| `platform.kafka.env` | `local` | Environment prefix for topic names |
| `platform.kafka.topic.raw` | _(derived)_ | Raw events topic (partition key: `tenantId:sessionId`) |
| `platform.kafka.topic.dlq` | _(derived)_ | Dead-letter topic for rejected and permanently failed events |

### Observability (OB-1 / OB-2 / OB-4 / OB-5)

| Property | Default | Description |
|:---------|:--------|:------------|
| `management.tracing.sampling.probability` | `1.0` (local) / `0.1` (prod) | Fraction of traces exported to OTEL Collector |
| `management.otlp.tracing.endpoint` | `http://otel-collector:4318/v1/traces` | OTLP HTTP endpoint — set to Jaeger/Tempo/Grafana Tempo in prod |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(env var)_ | Overrides OTLP endpoint per environment (set in docker-compose / K8s) |

**MDC Correlation (OB-2):** `TenantContextFilter` injects `requestId` (from `X-Request-Id` header or auto-generated UUID) and `tenantId` into SLF4J MDC for every request. `EventProducer` carries `requestId` across the Kafka boundary as `X-Request-Id` record header.

**DLQ Replay (OB-4):**

| Endpoint | Description |
|:---------|:------------|
| `POST /api/v1/admin/dlq/replay` | Re-publishes DLQ events to raw topic. Body: `{ "tenantId": "...", "reason": "...", "maxEvents": 1000 }` |
| `GET /api/v1/admin/dlq/stats` | Returns DLQ partition watermarks and total lag |

**Structured Logs (OB-5):** Spring profile `prod` activates `LogstashEncoder` JSON appender in `logback-spring.xml`, enriching every log line with `requestId`, `tenantId`, `service`, and `env` fields for Fluent Bit → Loki ingestion.

### Security (PASETO)

PASETO configuration is shared across all services via `shared-security` auto-configuration. Properties are bound by `PasetoProperties` in that module.

| Property | Local | Staging/Prod | Description |
|:---------|:------|:-------------|:------------|
| `platform.security.paseto.enabled` | `false` | `true` | Toggle in-service token verification |
| `platform.security.paseto.mode` | — | `local` | `local` = verify `v4.local` token; `public` = verify `v4.public` |
| `platform.security.paseto.local-key` | — | `${PASETO_LOCAL_KEY}` | 32-byte symmetric key (K8s Secret) |
| `platform.security.paseto.token-header` | `Authorization` | `X-Internal-Token` | Header carrying the token |
| `platform.security.paseto.issuer` | — | `edge` | Expected `iss` claim |
| `platform.security.paseto.audience` | — | `event-analysis` | Expected `aud` claim |
| `platform.security.paseto.clock-skew-seconds` | `60` | `30` | Allowed clock skew |

### Rate Limiting

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.rate-limit.enforced` | `false` | Enable blocking (`true` in staging/prod) |
| `platform.rate-limit.default-events-per-second` | `500` | Default per-tenant TPS limit |
| `platform.rate-limit.window-seconds` | `1` | Sliding window duration |

### Redis

| Property | Default | Description |
|:---------|:--------|:------------|
| `spring.data.redis.host` | `localhost` | Redis host (rate limiter) |
| `spring.data.redis.port` | `6379` | Redis port |
| `spring.data.redis.password` | _(blank)_ | Redis auth password (K8s Secret) |

---

## Running Locally

**Prerequisites:** Java 21, Docker (Kafka + Redis via Docker Compose).

```bash
# 1. Start dependencies
cd deploy
docker-compose up -d kafka redis

# 2. Run the service
cd ingestion-service
mvn spring-boot:run -Dspring-boot.run.profiles=local

# 3. Test ingest endpoint
curl -s -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-Context: walmart_us' \
  -d '{
    "eventId":   "e1",
    "userId":    "u1",
    "sessionId": "s1",
    "campaignId":"cmp_spring_99a",
    "eventType": "CLICK"
  }'
# → 202 Accepted {"status":"ACCEPTED","eventId":"e1",...}

# Missing tenant → 401
curl -s -i -X POST http://localhost:8080/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"e2","userId":"u2","sessionId":"s2","eventType":"CLICK"}'
```

**Actuator / Health:**
```bash
curl http://localhost:9090/actuator/health   # liveness
curl http://localhost:9090/actuator/prometheus # metrics
```

---

## Building & Testing

```bash
# Compile only
mvn -pl ingestion-service compile

# Run tests (unit + web-layer slice tests)
mvn -pl ingestion-service test

# Build fat JAR
mvn -pl ingestion-service package -DskipTests

# Docker image
docker build -t ingestion-service:latest ingestion-service/
```

### Test Coverage

| Test Class | Type | Covers |
|:-----------|:-----|:-------|
| `IngestControllerTest` | `@WebMvcTest` | 401 missing tenant, 422 blank field, 400 bad eventType, 202 accepted, 429 rate limit |

---

## Key Design Decisions

| Decision | Rationale |
|:---------|:----------|
| **DTO layer (`IngestEventRequest`)** | Decouples HTTP API contract from `ShoppingEvent` Kafka wire format; enables Jakarta `@Valid` constraints before the service layer is reached |
| **Service layer (`IngestionService`)** | Keeps controller thin (no business logic); service is independently testable; single responsibility per class |
| **`GlobalExceptionHandler`** | Centralized `@RestControllerAdvice` ensures every error, including `@Valid` failures, returns the same `ApiErrorResponse` envelope — no leaking of stack traces |
| **`write:events` scope via `AbstractPasetoAuthenticationFilter`** | Scope enforcement is declared in the filter subclass and applied by the shared base — consistent with how the query service declares `read:ads`, audited in one place |
| **Fail-closed PASETO startup guard** | Provided by `PasetoSecurityConfig` in `shared-security` — if auth is enabled but the key is absent the service refuses to start (OWASP A05/A07) |
| **DLQ for invalid schemas** | Events that pass DTO validation but fail `SchemaValidator` are routed to the dead-letter topic rather than returning 400, allowing SRE replay without data loss |
| **DLQ for permanent send failures** | `EventProducer` uses an idempotent Kafka producer (`enable.idempotence=true`) that retries within `delivery.timeout.ms` (2 min). On permanent broker rejection the event is forwarded to the DLQ via `DlqProducer` — no silent data loss |
| **Idempotent producer** | `enable.idempotence=true` + `max.in.flight=5` prevents broker-side duplicates when the producer retries a send. Combined with `acks=all` this provides at-least-once delivery with no duplicates under normal retry conditions |
| **`RequestLoggingFilterConfig`** | `CommonsRequestLoggingFilter` writes structured access-log entries gated at `DEBUG` level — provides request traceability without coupling application code to logging infrastructure |
| **MDC correlation (OB-2)** | `TenantContextFilter` sets `requestId` (from `X-Request-Id` or auto-UUID) and `tenantId` in SLF4J MDC. `EventProducer` carries the same `requestId` as a Kafka record header so downstream consumers can restore MDC context, enabling end-to-end log correlation across the async Kafka boundary |
| **Distributed tracing (OB-1)** | `KafkaProducerConfig` enables `template.setObservationEnabled(true)`. With `micrometer-tracing-bridge-otel` on the classpath, Spring Kafka automatically injects W3C `traceparent` header into every record and creates a Micrometer Observation span per send |
| **DLQ replay tooling (OB-4)** | `DlqReplayService` uses a one-shot `KafkaConsumer` (unique group ID per replay) to read DLQ events, filter by `tenantId`/`reason`, and re-publish to the raw topic. `DlqReplayController` exposes the replay and stats endpoints. `DlqReplayMetrics` exposes `ads_dlq_replayed_total` and `ads_dlq_replay_failed_total` counters |
| **Structured log aggregation (OB-5)** | `logback-spring.xml` activates `LogstashEncoder` JSON appender in the `prod` profile. JSON lines include `requestId`, `tenantId`, `service`, and `env` fields for Fluent Bit → Loki ingestion and correlated Grafana log queries |
