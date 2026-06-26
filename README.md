# Real-Time Streaming Insight Platform

> **A multi-tenant, real-time streaming analytics platform** for ad-network retailers.
> Ingests billions of ad-interaction events per day, processes them with stateful stream operators, and serves sub-second campaign metric queries across a three-tier storage hierarchy.
>
> **Status:** Current implementation · **SDD version:** 1.0 · **Last updated:** 2026-06-26

---

## Table of Contents

- [Platform Overview](#platform-overview)
- [Architecture at a Glance](#architecture-at-a-glance)
- [Modules](#modules)
- [Data Flow](#data-flow)
    - [Write Path](#write-path)
    - [Read Path](#read-path)
    - [Reconciliation Path](#reconciliation-path)
- [Kafka Topic Conventions](#kafka-topic-conventions)
- [Security Model](#security-model)
- [Observability](#observability)
- [Performance Targets](#performance-targets)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Building the Platform](#building-the-platform)
- [Related Documentation](#related-documentation)

---

## Platform Overview

This monorepo implements a **CQRS + Lambda hybrid** platform with fully separated write and read paths, an exactly-once stateful stream core (Apache Flink), and a periodic reconciliation layer for financial accuracy.

**Core capabilities:**

| Capability | Target |
|:-----------|:-------|
| Sustained ingest throughput | 150 K events/sec |
| Peak throughput (Black Friday) | 5 M events/sec |
| End-to-end freshness (P99) | < 3 seconds |
| Campaign query latency — Redis tier (P99) | < 7.5 ms |
| Campaign query latency — Pinot tier (P99) | < 85 ms |
| Daily event volume | ~13 billion events |
| Concurrent API requests | 25 000 |

---

## Architecture at a Glance

```
Client / SDK  ──HTTPS + PASETO v4.public──►  Kong API Gateway
                                                    │
                              verifies Ed25519 signature,
                              mints v4.local internal token,
                              injects X-Internal-Token + X-Tenant-Context
                                                    │
                    ┌───────────────────────────────┼───────────────────────────────┐
                    │           WRITE PATH           │            READ PATH           │
                    │                                │                                │
                    ▼                                │                 ▼              │
            ingestion-service                        │    insights-query-service      │
          (Spring Boot, port 8080)                   │   (Spring Boot, port 8080)     │
                    │                                │          │  │  │               │
          Avro-serialized ShoppingEvent              │     Redis Pinot Trino/Iceberg  │
                    │                                └───────────────────────────────┘
                    ▼
           Kafka — Raw Topic
                    │
                    ▼
     stream-processing-engine (Flink 1.19)
       ├── DeduplicationFunction  (RocksDB, 60-min TTL)
       ├── AttributionJoinFunction (24-h session window)
       └── Sinks:
             ├── Redis  (HINCRBY counters, hot < 48h)
             ├── Kafka Enriched Topic → Apache Pinot (warm < 30d)
             └── Iceberg / S3 (cold > 30d)

Shared libraries:  shared-model · shared-security
```

---

## Modules

| Module | Role | README |
|:-------|:-----|:-------|
| **`ingestion-service`** | CQRS write path — HTTP gateway, PASETO auth, rate limiting, schema validation, Kafka publish | [ingestion-service/README.md](ingestion-service/README.md) |
| **`stream-processing-engine`** | Apache Flink job — deduplication, sessionized attribution, multi-sink fan-out | [stream-processing-engine/README.md](stream-processing-engine/README.md) |
| **`insights-query-service`** | CQRS read path — tiered query routing (Redis → Pinot → Trino), reconciliation jobs | [insights-query-service/README.md](insights-query-service/README.md) |
| **`shared-model`** | Shared library — `ShoppingEvent` domain object, Avro SerDes, Kafka topic constants | [shared-model/README.md](shared-model/README.md) |
| **`shared-security`** | Shared library — PASETO v4.local / v4.public token utilities, filter base classes | [shared-security/README.md](shared-security/README.md) |

### ingestion-service

The public-facing HTTP gateway that accepts ad-interaction events. It runs the entire ingest pipeline — PASETO token verification, per-tenant Redis rate limiting, schema validation, and Avro-serialized publish to the Kafka raw topic. Invalid events are routed to a dead-letter topic for SRE replay rather than dropped.

Key design choices: thin controller + `IngestionService` interface; `GlobalExceptionHandler` for a consistent `ApiErrorResponse` envelope; fail-closed startup guard that refuses to launch when the PASETO key is missing in staging/prod.

### stream-processing-engine

The Apache Flink 1.19 stateful streaming job that consumes the raw Kafka topic and drives all downstream storage. It provides:

- **Deduplication** — `keyBy(eventId)` with RocksDB state and a 60-minute TTL, covering all SDK retry windows.
- **Attribution join** — `keyBy(sessionId)` with a 24-hour processing-time timer; synthesizes `CLICK_TO_BASKET` events when a `CLICK` is followed by an `ADD_TO_CART` within the same session.
- **Multi-sink fan-out** — Redis `HINCRBY` counters for hot queries, Kafka enriched topic for Pinot real-time ingestion, and Iceberg/S3 for cold archival.

Checkpoints are written to S3 every 30 seconds (incremental RocksDB), making the job safe on Spot/Preemptible instances.

### insights-query-service

The read-serving layer with tiered query routing:

| Tier | Store | Data age | P99 latency |
|:-----|:------|:---------|:------------|
| Hot | Redis `HGET` | < 48 h | < 7.5 ms |
| Warm | Apache Pinot (`COUNT*`) | < 30 d | < 85 ms |
| Cold | Trino over Iceberg/S3 | > 30 d | < 4.2 s |

Also hosts the `ReconciliationJob` — an hourly Redis-vs-Pinot drift check (auto-patches under-counts) and a daily Pinot-vs-Iceberg financial accuracy audit.

### shared-model

Plain-Java library with no Spring dependency. The single source of truth for:

- `ShoppingEvent` — the domain object flowing through every stage of the pipeline.
- `EventType` enum — with a null-safe, case-insensitive `EventType.from()` parser.
- Confluent Avro SerDes — schema defined as a `["null", "string"]`-union schema for forward-compatible evolution.
- `KafkaTopics` — topic and consumer-group name templates with a `resolve(env)` utility; prevents topic-name drift across services.

### shared-security

Shared PASETO utilities used by `ingestion-service` and `insights-query-service`. Provides the `PasetoAuthenticationFilter` base class, `PasetoProperties` binding, and the fail-closed `PasetoSecurityConfig` startup guard.

---

## Data Flow

### Write Path

```
Client SDK
  │  HTTPS POST /api/v1/events + PASETO v4.public
  ▼
Kong Gateway  →  verify Ed25519, mint v4.local, inject headers
  ▼
ingestion-service
  ├── PasetoAuthFilter    verify v4.local, enforce write:events scope
  ├── TenantContextFilter header presence check
  ├── TenantRateLimiter   Redis sliding window (500 TPS default)
  ├── SchemaValidator     eventId/userId/sessionId + EventType whitelist
  └── EventProducer       Avro-serialized ShoppingEvent → Kafka Raw Topic
                                │
                                ▼
                    stream-processing-engine (Flink)
                      ├── DeduplicationFunction → drops replays
                      ├── Redis HINCRBY          → hot counters
                      ├── Kafka Enriched Topic   → Pinot real-time
                      └── AttributionJoinFunction → CLICK_TO_BASKET events
```

### Read Path

```
Marketer / Dashboard
  │  GET /api/v1/campaigns/{id}/clicks?from=...&grain=hour
  ▼
Kong Gateway  →  verify v4.public, mint v4.local
  ▼
insights-query-service
  ├── PasetoAuthFilter        enforce read:ads scope
  ├── Campaign allow-list     enforced from allowed_campaigns JWT claim
  └── TierRoutingEngine
        ├── < 48h   → Redis  HGET campaign:{tenantId}:{campaignId}
        ├── < 30d   → Pinot  SELECT COUNT(*) WHERE tenant_id=? AND campaign_id=?
        └── > 30d   → Trino  JDBC over Iceberg/S3 Parquet partitions
```

### Reconciliation Path

| Job | Schedule | What it compares | Auto-action |
|:----|:---------|:----------------|:------------|
| Hourly | `0 5 * * * *` | Redis counters vs Pinot, last 2 h | Auto-patch `HINCRBY` for under-counts; warn-only for over-counts |
| Daily | `0 15 1 * * *` | Pinot vs Iceberg, previous UTC day | Alert if drift > 0.01%; store `ReconciliationReport` |

SRE can trigger an on-demand run via `POST /api/v1/reconciliation/runs/hourly`.

---

## Kafka Topic Conventions

All topic names follow the template:

```
{env}.{visibility}.{topic-type}.{domain}.{subdomain}.{record-name}-by-{key-name}[-v{N}]
```

| Topic | Resolved example (prod) | Producer | Consumer |
|:------|:------------------------|:---------|:---------|
| Raw | `prod.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session` | `ingestion-service` | Flink job |
| DLQ | `prod.internal.event.ads.clickstream.ad-interaction-failed-by-tenant-id` | `ingestion-service` | SRE tooling |
| Enriched | `prod.internal.event.ads.attribution.ad-interaction-enriched-by-campaign` | Flink job | Pinot + `insights-query-service` |
| Reconciliation corrections | `prod.internal.command.ads.reconciliation.counter-correction-by-campaign` | reconciliation job | SRE replay tooling |

> Never hardcode topic names. Use Spring property placeholders (`${platform.kafka.topic.raw}`). The `{env}` segment is injected at runtime via `KAFKA_ENV`.

---

## Security Model

The platform uses **edge-validated PASETO with internal token re-minting (Option C)**:

```
External v4.public token  →  Kong verifies Ed25519 signature
                          →  Mints short-lived v4.local internal token (60 s TTL)
                          →  Forwards X-Internal-Token + X-Tenant-Context

In each microservice:
  PasetoAuthenticationFilter (@Order 0)
    └─ verifies v4.local with shared symmetric key
    └─ enforces required scope (write:events or read:ads)
    └─ rewrites X-Tenant-Context from verified claims — never trusts request body
  TenantContextFilter (@Order 1)
    └─ defense-in-depth: rejects any request missing the tenant header
```

**Environment behaviour:**

| Profile | PASETO enabled | Key source |
|:--------|:--------------|:-----------|
| `local` | No (pass-through) | — |
| `staging` / `prod` | Yes | `PASETO_LOCAL_KEY` (K8s Secret) |

The service **refuses to start** if `PASETO_LOCAL_KEY` is absent when auth is enabled (fail-closed guard).

Multi-tenant isolation is enforced at every layer: `tenantId` is extracted from the verified token (never the request body), Redis keys are namespaced `campaign:{tenantId}:{campaignId}`, Pinot queries inject `AND tenant_id = ?` as an RLS predicate, and Iceberg data is S3-partitioned by `tenant_id` for sovereignty.

---

## Observability

All services expose Micrometer metrics on the actuator port (`9090`) at `/actuator/prometheus`.

**SLOs:**

| SLI | SLO Target | Alert threshold |
|:----|:-----------|:----------------|
| Ingress availability | > 99.99% | 5xx rate > 0.1% |
| End-to-end freshness (P95) | < 3.0 s | P95 > 10 s |
| Read API latency (P95) | < 200 ms | P95 > 500 ms for 3 min |
| Data correctness drift (daily) | 0.00% | > 0.05% |
| Flink consumer lag | < 100 K messages | > 500 K messages |

**Key metrics by service:**

| Service | Metric |
|:--------|:-------|
| `ingestion-service` | `ads.ingest.events.accepted`, `ads.ingest.events.dlq`, `ads.ingest.rate.limited`, `ads.ingest.latency.ms` |
| `stream-processing-engine` | `ads.events.processed`, `ads.events.dedup.dropped`, `ads.events.attributed`, `flink.checkpoint.duration`, `flink.consumer.lag` |
| `insights-query-service` | `ads.queries.latency.ms`, `ads.cache.hits`, `ads.tier.redis.count`, `ads.tier.pinot.count`, `ads.tier.trino.count` |
| Reconciliation | `ads.reconciliation.discrepancies`, `ads.reconciliation.auto.patches`, `ads.reconciliation.campaigns.checked` |

---

## Performance Targets

### Auto-scaling

| Component | Min pods | Max pods | Scale trigger |
|:----------|:---------|:---------|:--------------|
| `ingestion-service` | 10 | 300 | CPU > 75% |
| `insights-query-service` | 3 | 50 | CPU > 70% |
| Flink `maxParallelism` | — | 1024 | TaskManager arrival (S3 checkpoint restore) |
| Kafka partitions (per topic) | 128 | 128 | Fixed; bounds Flink parallelism |

### Flink fault tolerance

Checkpoints every 30 seconds to S3 (incremental RocksDB). On TaskManager failure or Spot reclaim: restore from latest checkpoint, replay Kafka from checkpointed offset, `DeduplicationFunction` drops re-delivered events. Exactly-once guaranteed via Kafka transactional sink committed inside the checkpoint barrier.

---

## Repository Layout

```
/
├── ingestion-service/          # Spring Boot — CQRS write path
├── stream-processing-engine/   # Apache Flink 1.19 job
├── insights-query-service/     # Spring Boot — CQRS read path + reconciliation
├── shared-model/               # Plain-Java — ShoppingEvent, Avro SerDes, KafkaTopics
├── shared-security/            # Plain-Java — PASETO filters and config
├── deploy/
│   ├── docker-compose.yml      # Local Kafka, Redis, Schema Registry, Pinot
│   └── k8s/                    # Helm charts and K8s manifests
├── docs/
│   ├── SDD.md                  # System Design Document (full LLD + HLD)
│   ├── AUTHENTICATION.md       # PASETO Option-C token exchange detail
│   ├── architecture.md         # Diagrams and ADRs
│   └── implementation-details.md
└── pom.xml                     # Parent Maven BOM
```

---

## Getting Started

**Prerequisites:** Java 21, Maven 3.9+, Docker (for local dependencies).

```bash
# 1. Start local infrastructure (Kafka, Redis, Schema Registry)
cd deploy
docker-compose up -d kafka redis schema-registry

# 2. Build shared libraries first (required by services)
mvn -pl shared-model,shared-security install -DskipTests

# 3. Run ingestion-service (write path)
mvn -pl ingestion-service spring-boot:run -Dspring-boot.run.profiles=local

# 4. Run insights-query-service (read path)
mvn -pl insights-query-service spring-boot:run -Dspring-boot.run.profiles=local

# 5. Submit the Flink job (stream processing)
# See stream-processing-engine/README.md for Flink cluster setup

# 6. Send a test event
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
# → 202 Accepted {"status":"ACCEPTED","eventId":"e1","remainingQuota":499}
```

**Actuator endpoints** (port `9090`, internal cluster only):

```bash
curl http://localhost:9090/actuator/health      # liveness probe
curl http://localhost:9090/actuator/prometheus  # Prometheus metrics scrape
```

**API docs** (per service, port `8080`):

```
http://localhost:8080/swagger-ui.html
http://localhost:8080/v3/api-docs
```

---

## Building the Platform

```bash
# Build everything in dependency order
mvn install -DskipTests

# Build and test a single module
mvn -pl ingestion-service test
mvn -pl shared-model install -DskipTests

# Build Docker images
docker build -t ingestion-service:latest ingestion-service/
docker build -t insights-query-service:latest insights-query-service/
docker build -t stream-processing-engine:latest stream-processing-engine/
```

### Module dependency order

```
shared-model  ──►  ingestion-service
     │         └─► stream-processing-engine
     │         └─► insights-query-service
     │
shared-security ─► ingestion-service
               └─► insights-query-service
```

Always install `shared-model` and `shared-security` before building the service modules.

---

## Related Documentation

| Document | Location | Contents |
|:---------|:---------|:---------|
| System Design Document | [`docs/SDD.md`](docs/SDD.md) | Full HLD + LLD, all sequence diagrams, algorithm walkthroughs, trade-off table |
| Authentication Design | [`docs/AUTHENTICATION.md`](docs/AUTHENTICATION.md) | PASETO Option-C token exchange detail, threat model |
| ingestion-service | [`ingestion-service/README.md`](ingestion-service/README.md) | API reference, security config, rate limiting, local dev guide |
| stream-processing-engine | [`stream-processing-engine/README.md`](stream-processing-engine/README.md) | Flink job config, state management, checkpoint tuning |
| insights-query-service | [`insights-query-service/README.md`](insights-query-service/README.md) | Query API reference, tier routing config, reconciliation API |
| shared-model | [`shared-model/README.md`](shared-model/README.md) | `ShoppingEvent` field reference, Avro schema, topic catalogue |
| shared-security | [`shared-security/README.md`](shared-security/README.md) | PASETO filter usage, config properties |
