# Real-Time Streaming Insight Platform


Real-Time Streaming Analytics Platform is a production-grade, cloud-native, multi-tenant analytics system inspired by large-scale retail advertising platforms such as Amazon Ads, Flipkart Ads, and Walmart Connect. It demonstrates how modern event-driven architectures process massive volumes of user interaction data to generate real-time business insights.

The platform ingests high-velocity clickstream and ad engagement events, performs stateful click-to-basket attribution using distributed stream processing, incrementally materializes campaign metrics into an OLAP store, and exposes secure, low-latency REST APIs for real-time analytics and reporting.

Designed with scalability, resilience, and extensibility in mind, the project showcases industry-standard architectural patterns, including event-driven microservices, distributed messaging, stream processing, exactly-once semantics, real-time aggregation, and cloud-native deployment. It serves as a reference implementation for building enterprise-scale streaming data platforms capable of handling millions of events with low latency and high reliability.

>  Full system design: [`docs/SDD.md`](docs/SDD.md)

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Architecture at a Glance](#2-architecture-at-a-glance)
- [3. Monorepo Layout](#3-monorepo-layout)
- [4. Modules & Responsibilities](#4-modules--responsibilities)
- [5. Tech Stack](#5-tech-stack)
- [6. Prerequisites](#6-prerequisites)
- [7. Build](#7-build)
- [8. Run Locally (Docker Compose)](#8-run-locally-docker-compose)
- [9. Run Services Individually](#9-run-services-individually)
- [10. API Reference](#10-api-reference)
- [11. End-to-End Walkthrough](#11-end-to-end-walkthrough)
- [12. Configuration & Environments](#12-configuration--environments)
- [13. Kubernetes Deployment](#13-kubernetes-deployment)
- [14. Testing](#14-testing)
- [15. Troubleshooting](#15-troubleshooting)
- [16. Related Documentation](#16-related-documentation)

---

## 1. Overview

The platform is split along **CQRS** lines: a write path that absorbs traffic spikes, a stream path that does stateful processing, and a read path optimized for fast dashboard queries.

| Capability | How it's delivered |
| :--- | :--- |
| High-velocity ingestion | Stateless `ingestion-service` → Kafka, partitioned by `tenant_id + session_id` |
| Deduplication | TTL-bounded keyed map in the stream engine (Flink-style) |
| Click-to-basket attribution | Sessionized stateful join within a configurable window (default 24h) |
| Low-latency insights | Pre-aggregated OLAP store (Apache Pinot Star-Tree; in-memory stand-in locally) |
| Multi-tenancy | In-service PASETO `v4.public` verification + `X-Tenant-Context` enforced at write and read controllers |
| Per-env config | Spring Profiles + Kustomize overlays (local / dev / staging / prod) |

---

## 2. Architecture at a Glance

```
 Clients/SDKs
     │  HTTP JSON (+ X-Tenant-Context)
     ▼
┌─────────────────────┐   produce (key = tenant:session)   ┌───────────────────────────┐
│  ingestion-service  │ ─────────────────────────────────► │  Kafka: events.shopping.raw│
│  (Write / Command)  │                                    └─────────────┬─────────────┘
└─────────────────────┘                                                  │ consume
                                                                         ▼
                                          ┌─────────────────────────────────────────────┐
                                          │           stream-processing-engine          │
                                          │  deduplicate → sessionize → click↔basket join│
                                          └─────────────┬───────────────────────────────┘
                                                        │ produce enriched + conversions
                                                        ▼
                                          ┌───────────────────────────────────────────┐
                                          │       Kafka: events.shopping.aggregates    │
                                          └─────────────┬─────────────────────────────┘
                                                        │ consume → index
                                                        ▼
 Marketers ──GET /ad/{id}/…──►  ┌─────────────────────────────────────┐
                                │        insights-query-service        │
                                │  StarTree store → CQRS read APIs     │
                                └─────────────────────────────────────┘
```

> In production the stream engine sinks to **Apache Pinot** + **Redis**; locally the `insights-query-service` consumes the `aggregates` topic into an in-memory Star-Tree store so the full loop runs without external OLAP infra.

---

## 3. Monorepo Layout

```
media-pulse-iq/                       # Maven parent POM (packaging: pom)
├── pom.xml
├── README.md                         # This file
│
├── docs/
│   └── SDD.md                        # System design
│   └── AUTHENTICATION.md             # Authentication Details
│
├── shared-model/                     # Shared contracts (DTOs, enums, topic constants, Redis key schema)
│   └── src/main/java/com/java/model/
│       ├── ShoppingEvent.java
│       ├── EventType.java
│       ├── constants/KafkaTopics.java
│       └── redis/RedisKeySchema.java  # Canonical Redis key/field patterns (shared between write and read paths)
│
├── shared-security/                  # Security library (PASETO crypto, shared filter base, PII utilities)
│   └── src/main/java/com/java/security/
│       ├── paseto/
│       │   ├── AbstractPasetoAuthenticationFilter.java  # Base OncePerRequestFilter: verify, scope, audit, tenant rewrite
│       │   ├── PasetoProperties.java                    # Shared @ConfigurationProperties (auto-configured)
│       │   ├── PasetoSecurityConfig.java                # Spring Boot auto-configuration: PasetoVerifier + key guard
│       │   ├── PasetoVerifier.java / PasetoClaims.java
│       │   ├── PasetoV4PublicVerifier.java / PasetoV4LocalVerifier.java
│       │   └── PasetoV4Local.java / PasetoV4LocalIssuer.java / …
│       └── pii/PiiMasker.java
│
├── ingestion-service/                # Write path (REST → Kafka)
│   ├── Dockerfile
│   └── src/main/java/com/java/ingestion/
│       ├── IngestionApplication.java
│       ├── config/
│       │   ├── KafkaProducerConfig.java
│       │   └── RequestLoggingFilterConfig.java         # CommonsRequestLoggingFilter (DEBUG-gated access log)
│       ├── controller/IngestController.java
│       ├── producer/EventProducer.java + DlqProducer.java
│       ├── ratelimit/TenantRateLimiter.java
│       ├── security/
│       │   ├── PasetoAuthenticationFilter.java         # Thin subclass: declares write:events scope
│       │   └── TenantContextFilter.java
│       ├── service/IngestionService.java + IngestionServiceImpl.java
│       └── validation/SchemaValidator.java
│
├── stream-processing-engine/         # Stream path (Kafka consumer + stateful join)
│   ├── Dockerfile
│   └── src/main/java/com/java/processing/
│       ├── ProcessingApplication.java
│       ├── consumer/EventConsumer.java
│       ├── job/FlinkStreamingJob.java
│       ├── operator/DeduplicationFunction.java + AttributionJoinFunction.java + SerDes
│       ├── sink/
│       │   ├── RedisHotCounterSink.java                # Uses RedisKeySchema from shared-model
│       │   ├── KafkaSink / IcebergS3Sink / PinotSink
│       └── config/FlinkJobLauncher.java + FlinkProperties.java
│
├── insights-query-service/           # Read path (CQRS REST APIs)
│   ├── Dockerfile
│   └── src/main/java/com/java/query/
│       ├── QueryApplication.java
│       ├── controller/AdInsightsController.java
│       ├── handler/
│       │   ├── TierQueryHandler.java                   # Strategy interface per serving tier
│       │   ├── RedisCacheTierHandler.java              # Hot tier — uses RedisKeySchema
│       │   ├── PinotOlapTierHandler.java               # Warm tier — Pinot native SDK
│       │   ├── StarTreeFallbackResolver.java           # In-memory fallback (local/dev)
│       │   └── TrinoLakehouseTierHandler.java          # Cold tier — Trino JDBC / Iceberg
│       ├── observability/
│       │   ├── QueryMetricsAspect.java                 # AOP @TieredQuery instrumentation
│       │   ├── TierQueryContext.java                   # ThreadLocal tier context for AOP
│       │   └── TieredQuery.java                        # Method annotation
│       ├── reconciliation/
│       │   ├── ReconciliationStrategy.java             # Strategy interface
│       │   ├── HourlyReconciliationStrategy.java       # Redis vs Pinot
│       │   ├── DailyReconciliationStrategy.java        # Pinot vs Iceberg
│       │   └── ReconciliationJob.java / ReconciliationStore.java
│       ├── router/TierRoutingEngine.java + TierRoutingProperties.java
│       ├── security/PasetoAuthenticationFilter.java    # Thin subclass: declares read:ads scope
│       ├── service/InsightsServiceImpl.java + TieredInsightsEngine.java + InsightsRequestValidator.java
│       └── store/RedisInsightsStore.java + PinotRestClient.java + TrinoIcebergClient.java
│
├── deploy/
│   ├── docker-compose.yml            # Local full stack
│   ├── config/                       # Reference per-env Spring config templates
│   └── k8s/
│       ├── base/                     # Deployments, Services, HPA/KEDA
│       └── overlays/{dev,staging,prod}/   # Kustomize overlays + configmap.env
│
└── docs/
    ├── AUTHENTICATION.md              # Auth model: edge-validated PASETO + in-service verify-everywhere
    ├── SECURITY-OWASP.md              # OWASP Top 10 risk analysis + remediation status
    └── postman_collection.json
```

Each service also ships `application.yml` + `application-{local,dev,staging,prod}.yml` under `src/main/resources/`.

---

## 4. Modules & Responsibilities

| Module | Type | Port | Responsibility |
| :--- | :--- | :--- | :--- |
| **shared-model** | Library JAR | – | `ShoppingEvent` schema, `EventType`, `KafkaTopics` constants, `RedisKeySchema` (DRY across services) |
| **shared-security** | Library JAR (Spring Boot auto-config) | – | PASETO crypto primitives, `AbstractPasetoAuthenticationFilter` base class, `PasetoProperties` + `PasetoSecurityConfig` auto-configuration, `PiiMasker` |
| **ingestion-service** | Spring Boot REST + Kafka producer | `8080` | Validates payload, enforces tenant context (`write:events`), rate-limits per tenant, publishes to `events.shopping.raw` keyed by `tenant:session` |
| **stream-processing-engine** | Spring Boot Kafka consumer | `8082` | Dedup → sessionized click→basket join → emits enriched events to `events.shopping.aggregates`; writes Redis hot-counters via `RedisKeySchema` |
| **insights-query-service** | Spring Boot REST + Kafka consumer | `8083` | Indexes aggregates into tiered store (Redis / Pinot / Trino-Iceberg); serves clicks / impressions / clickToBasket APIs with tier-routing strategy pattern |

---

## 5. Tech Stack

| Concern | Technology |
| :--- | :--- |
| Language / Runtime | Java 21 |
| Framework | Spring Boot 3.5.x (Web, Actuator), Spring Kafka |
| Boilerplate | Lombok (getters/setters/builders, constructor injection, `@Slf4j`) |
| Messaging | Apache Kafka |
| Serving (prod) | Apache Pinot (Star-Tree OLAP), Redis cache, Trino + Iceberg for cold data |
| Build | Maven (multi-module) |
| Containers | Docker (multi-stage), Docker Compose |
| Orchestration | Kubernetes + Kustomize, HPA + KEDA autoscaling |
| Testing | JUnit 5, Spring MockMvc |

---

## 6. Prerequisites

- **JDK 21+**
- **Maven 3.9+**
- **Docker + Docker Compose** (for the local full stack)
- **kubectl + a cluster** (optional, for K8s deployment)

---

## 7. Build

Build all modules from the repository root:

```bash
mvn clean install
```

Build a single module (and its dependencies):

```bash
mvn -pl ingestion-service -am clean package
```

> ℹ️ The build resolves Spring Boot from Maven Central. On a restricted network, point Maven at an accessible mirror (or set `<offline>` in `~/.m2/settings.xml` once the dependency closure is cached).

---

## 8. Run Locally (Docker Compose)

Bring up Kafka and all three services with the `local` profile:

```bash
cd deploy
docker compose up --build
```

Exposed ports:

| Service | URL |
| :--- | :--- |
| ingestion-service | http://localhost:8081 (container 8080) |
| stream-processing-engine | http://localhost:8082 |
| insights-query-service | http://localhost:8083 |

---

## 9. Run Services Individually

Each service is a standalone Spring Boot app. Start Kafka first (e.g. via the compose file), then:

```bash
mvn -pl ingestion-service        spring-boot:run -Dspring-boot.run.profiles=local
mvn -pl stream-processing-engine spring-boot:run -Dspring-boot.run.profiles=local
mvn -pl insights-query-service   spring-boot:run -Dspring-boot.run.profiles=local
```

---

## 10. API Reference

All endpoints require the `X-Tenant-Context` header (injected by the gateway after PASETO validation; when `platform.security.paseto.enabled=true` each service also re-verifies the token itself and derives the tenant from the verified claims). Requests without a valid identity return **401**. See [`docs/AUTHENTICATION.md`](docs/AUTHENTICATION.md) for the full model.

### Ingest an event — `POST /v1/events/ingest` (ingestion-service)

```bash
curl -X POST http://localhost:8081/v1/events/ingest \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Context: walmart_us" \
  -d '{
    "eventId": "evt_click_1",
    "userId": "user_a",
    "sessionId": "sess_a",
    "campaignId": "spring_sale_2026",
    "eventType": "CLICK",
    "cost": 0.45
  }'
```
Response: `202 Accepted` with `{ "status": "ACCEPTED", "eventId": "...", "processedTimestamp": "..." }`.

Supported `eventType` values: `IMPRESSION`, `CLICK`, `PRODUCT_VIEW`, `ADD_TO_CART`, `PURCHASE`.

### Campaign insights (insights-query-service, port 8083)

| Method & Path | Description |
| :--- | :--- |
| `GET /ad/{campaignID}/clicks` | Click count for a campaign |
| `GET /ad/{campaignID}/impressions` | Impression count for a campaign |
| `GET /ad/{campaignID}/clickToBasket` | Attributed click→basket conversions |

Common query params: `from`, `to` (ISO-8601), `grain` (`minute|hour|day`).

```bash
curl "http://localhost:8083/ad/spring_sale_2026/clicks?grain=hour" \
  -H "X-Tenant-Context: walmart_us"
```

Example response:
```json
{
  "tenantId": "walmart_us",
  "campaignId": "spring_sale_2026",
  "metric": "click",
  "from": "2026-06-24T10:00:00Z",
  "to": "2026-06-24T12:00:00Z",
  "grain": "hour",
  "series": [{ "timestamp": "2026-06-24T12:00:00Z", "value": 1 }],
  "total": 1,
  "source": "ApachePinot"
}
```

> A ready-to-import Postman collection lives at [`docs/postman_collection.json`](docs/postman_collection.json).

---

## 11. End-to-End Walkthrough

With the local stack running, post a `CLICK` and then an `ADD_TO_CART` on the **same `sessionId`** to trigger an attributed conversion:

```bash
BASE_ING=http://localhost:8081
BASE_QRY=http://localhost:8083
H='-H Content-Type:application/json -H X-Tenant-Context:walmart_us'

# 1) Ad click
curl -X POST $BASE_ING/v1/events/ingest $H \
  -d '{"eventId":"e1","userId":"u1","sessionId":"s1","campaignId":"cmp1","eventType":"CLICK","cost":0.5}'

# 2) Add to cart (same session, within attribution window)
curl -X POST $BASE_ING/v1/events/ingest $H \
  -d '{"eventId":"e2","userId":"u1","sessionId":"s1","eventType":"ADD_TO_CART"}'

# 3) Read back the conversion
curl "$BASE_QRY/ad/cmp1/clickToBasket?grain=hour" -H "X-Tenant-Context: walmart_us"
# → total: 1  (the stream engine attributed the basket add to the prior click)
```

---

## 12. Configuration & Environments

Built once, promoted everywhere — behaviour changes only via configuration. See [`implementation-details.md` 10](implementation-details.md#10-environment-configuration-management-local--dev--staging--prod).

| | local | dev | staging | prod |
| :--- | :--- | :--- | :--- | :--- |
| Profile | `local` | `dev` | `staging` | `prod` |
| Kafka | `localhost:9092` | `kafka-dev:9092` | `kafka-staging:9092` | `kafka-prod-msk:9094` (TLS) |
| Partitions | 1 | 6 | 64 | 128 |
| Logging | `DEBUG` | `DEBUG` | `INFO` | `WARN` |

Activate a profile:

```bash
# Maven
mvn -pl ingestion-service spring-boot:run -Dspring-boot.run.profiles=dev
# Container / K8s
SPRING_PROFILES_ACTIVE=prod
```

Secrets are never committed — credentials are injected at runtime via K8s Secrets (External Secrets Operator / Vault / AWS Secrets Manager).

---

## 13. Kubernetes Deployment

Manifests use **Kustomize** (`deploy/k8s/base` + per-env overlays). Each overlay generates an `app-config` ConfigMap from its `configmap.env` and sets the active profile/replicas.

```bash
# Dev
kubectl apply -k deploy/k8s/overlays/dev
# Staging
kubectl apply -k deploy/k8s/overlays/staging
# Prod
kubectl apply -k deploy/k8s/overlays/prod
```

Autoscaling:
- `ingestion-service` & `insights-query-service` → **HPA** (CPU).
- `stream-processing-engine` → **KEDA** ScaledObject on Kafka consumer lag.

---

## 14. Testing

Run the full test suite:

```bash
mvn test
```

Coverage highlights:
- `stream-processing-engine`: `StatefulJoinerTest` (attribution window, double-attribution guard), `DeduplicatorTest` (TTL eviction).
- `ingestion-service`: `IngestControllerTest` (401 without tenant, validation, async dispatch).
- `insights-query-service`: `AdInsightsControllerTest` (tenant enforcement, response shape), `ReconciliationControllerTest` (paged reports, on-demand run), `ReconciliationJobTest` (hourly/daily strategies).
- `shared-security`: `PasetoV4PublicVerifierTest`, `PasetoV4LocalVerifierTest`, `PiiMaskerTest` (pure unit tests).

---

## 15. Troubleshooting

| Symptom | Likely cause / fix |
| :--- | :--- |
| `401 Unauthorized` on any call | Missing `X-Tenant-Context` header — add it to the request |
| Conversion not counted | `CLICK` and `ADD_TO_CART` must share the same `sessionId` and fall within the attribution window |
| Services can't reach Kafka locally | Ensure `docker compose up` is healthy; brokers advertise `kafka:9092` inside the compose network |
| Maven can't download Spring Boot | Restricted network — use an accessible mirror or pre-populate `~/.m2` then build with `-o` (offline) |
| Duplicate events inflating counts | Expected to be dropped by the deduplicator within its TTL window |

---

## 16. Related Documentation

| Document | Location | Contents |
|:---------|:---------|:---------|
| System Design Document | [`docs/SDD.md`](docs/SDD.md) | Full HLD + LLD, all sequence diagrams, algorithm walkthroughs, trade-off table |
| Authentication Design | [`docs/AUTHENTICATION.md`](docs/AUTHENTICATION.md) | PASETO Option-C token exchange detail, threat model |
| ingestion-service | [`ingestion-service/README.md`](ingestion-service/README.md) | API reference, security config, rate limiting, local dev guide |
| stream-processing-engine | [`stream-processing-engine/README.md`](stream-processing-engine/README.md) | Flink job config, state management, checkpoint tuning |
| insights-query-service | [`insights-query-service/README.md`](insights-query-service/README.md) | Query API reference, tier routing config, reconciliation API |
| shared-model | [`shared-model/README.md`](shared-model/README.md) | `ShoppingEvent` field reference, Avro schema, topic catalogue |
| shared-security | [`shared-security/README.md`](shared-security/README.md) | PASETO filter usage, config properties |

