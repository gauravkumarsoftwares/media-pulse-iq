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
| **Authentication** | `PasetoAuthenticationFilter` — verifies in-service PASETO `v4.local` token (Option C), enforces `write:events` scope |
| **Tenant isolation** | `TenantContextFilter` — defense-in-depth header presence check; controller re-check |
| **Rate limiting** | `TenantRateLimiter` — Redis sliding-window counter, per-tenant TPS limit |
| **Schema validation** | `SchemaValidator` — mandatory field presence + recognized `EventType` |
| **DLQ routing** | `DlqProducer` — invalid events sent to the dead-letter topic for SRE replay |
| **Event publish** | `EventProducer` — Avro-serialized `ShoppingEvent` to the Kafka raw topic |
| **Observability** | `IngestionMetrics` — Micrometer counters/timers exposed via Prometheus |

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
│   └── KafkaProducerConfig.java   # KafkaTemplate + ProducerFactory wiring
│
├── controller/
│   └── IngestController.java      # POST /api/v1/events — thin controller, delegates to IngestionService
│
├── dto/
│   ├── IngestEventRequest.java    # Input DTO with Jakarta Bean Validation (@NotBlank, @Size, @DecimalMin)
│   └── IngestEventResponse.java   # Acceptance confirmation {status, eventId, processedTimestamp, remainingQuota}
│
├── observability/
│   └── IngestionMetrics.java      # Micrometer: events_accepted_total, events_validation_failed_total, etc.
│
├── producer/
│   ├── DlqProducer.java           # Publishes rejected events to the DLQ Kafka topic
│   └── EventProducer.java         # Publishes validated events to the raw Kafka topic
│
├── ratelimit/
│   ├── RateLimitProperties.java   # Binds platform.rate-limit.* config
│   └── TenantRateLimiter.java     # Redis-backed sliding-window rate limiter (per tenant)
│
├── security/
│   ├── PasetoAuthenticationFilter.java # @Order(0): verifies PASETO token, enforces write:events scope
│   ├── PasetoProperties.java           # Binds platform.security.paseto.*
│   ├── PasetoSecurityConfig.java       # Builds PasetoVerifier bean; fail-closed startup guard
│   └── TenantContextFilter.java        # @Order(1): tenant header presence check
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
  "processedTimestamp": "2026-06-26T08:00:00Z",
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
  "timestamp":   "2026-06-26T08:00:00.000Z",
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
→ PasetoAuthenticationFilter verifies v4.local with shared key
→ enforces write:events scope
→ rewrites X-Tenant-Context from verified claims
→ TenantContextFilter: defense-in-depth header presence check
```

**In local dev** (`spring.profiles.active=local`): `platform.security.paseto.enabled=false` — the filter passes through and the service trusts the gateway-injected `X-Tenant-Context` header directly.

**In staging/prod**: `mode=local` + `local-key=${PASETO_LOCAL_KEY}` — the service verifies the `v4.local` symmetric token in-process. The service **refuses to start** if `PASETO_LOCAL_KEY` is missing (fail-closed guard).

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
| `spring.kafka.producer.acks` | `all` | Durability: all replicas must ack |
| `platform.kafka.env` | `local` | Environment prefix for topic names |
| `platform.kafka.topic.raw` | _(derived)_ | Raw events topic |
| `platform.kafka.topic.dlq` | _(derived)_ | Dead-letter topic |

### Security (PASETO)

| Property | Local | Staging/Prod | Description |
|:---------|:------|:-------------|:------------|
| `platform.security.paseto.enabled` | `false` | `true` | Toggle in-service token verification |
| `platform.security.paseto.mode` | — | `local` | `local` = verify `v4.local` token; `public` = verify `v4.public` |
| `platform.security.paseto.local-key` | — | `${PASETO_LOCAL_KEY}` | 32-byte symmetric key (K8s Secret) |
| `platform.security.paseto.token-header` | `Authorization` | `X-Internal-Token` | Header carrying the token |
| `platform.security.paseto.issuer` | — | `edge` | Expected `iss` claim |
| `platform.security.paseto.audience` | — | `media-pulse-iq` | Expected `aud` claim |
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
| **`write:events` scope enforcement** | Ingestion filter enforces the token's ingest authorization scope before any work is done, complementing the gateway's edge-level scope check (OWASP A01) |
| **Fail-closed PASETO startup guard** | If auth is enabled but the symmetric key is absent, the service refuses to start — prevents silent auth bypass on misconfigured deployments (OWASP A05/A07) |
| **DLQ for invalid schemas** | Events that pass DTO validation but fail `SchemaValidator` are routed to the dead-letter topic rather than returning 400, allowing SRE replay without data loss |

