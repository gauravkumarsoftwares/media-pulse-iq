# stream-processing-engine

> **Role in the platform:** Real-time stream processor — consumes raw ad-interaction events from Kafka, deduplicates them, synthesizes `CLICK_TO_BASKET` conversions via sessionized attribution joins, and fans out to three downstream sinks: Kafka enriched topic (→ Apache Pinot), Redis hot-counter cache, and Iceberg/S3 cold archive.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Processing Pipeline](#processing-pipeline)
  - [1. KafkaSource](#1-kafkasource)
  - [2. Deduplication (DeduplicationFunction)](#2-deduplication-deduplicationfunction)
  - [3. Attribution Join (AttributionJoinFunction)](#3-attribution-join-attributionjoinfunction)
  - [4. Sinks](#4-sinks)
- [Package Structure](#package-structure)
- [Configuration Reference](#configuration-reference)
- [State Management](#state-management)
- [Fault Tolerance & Checkpointing](#fault-tolerance--checkpointing)
- [Deployment Modes](#deployment-modes)
- [Running Locally](#running-locally)
- [Building & Testing](#building--testing)
- [Key Design Decisions](#key-design-decisions)

---

## Architecture Overview

```
Kafka Raw Topic
{env}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session
    │
    │  ShoppingEventDeserializationSchema (Confluent Avro)
    │  WatermarkStrategy.forBoundedOutOfOrderness(5 min)
    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    stream-processing-engine                          │
│                                                                     │
│  keyBy(eventId)                                                     │
│  DeduplicationFunction                                              │
│    └─ RocksDB ValueState + TTL (default 60 min)                    │
│    └─ Drops duplicate eventIds within the TTL window               │
│                              │                                      │
│                    unique events only                               │
│                              │                                      │
│               ┌──────────────┤                                      │
│               │              │                                      │
│               ▼              ▼                                      │
│       KafkaSink          RedisHotCounterSink                        │
│   (enriched topic)     HINCRBY campaign:{tenant}:{campaign}        │
│    → Pinot ingest      TTL refresh (default 48 h)                  │
│               │                                                     │
│               └──── keyBy(sessionId)                               │
│                     AttributionJoinFunction                         │
│                       └─ ValueState lastClick + attributed guard   │
│                       └─ ProcessingTimeTimer (attribution window)  │
│                       └─ Synthesizes CLICK_TO_BASKET events        │
│                              │                                      │
│               ┌──────────────┘                                      │
│               │                                                     │
│               ▼                                                     │
│       KafkaSink          RedisHotCounterSink                        │
│   (enriched topic)     (CLICK_TO_BASKET conversions)               │
└─────────────────────────────────────────────────────────────────────┘
                │
                ▼
   Kafka Enriched Topic
   {env}.internal.event.ads.attribution.ad-interaction-enriched-by-campaign
                │
    ┌───────────┤
    │           │
    ▼           ▼
 Pinot       insights-query-service
 Realtime    AggregateConsumer (Spring-Kafka)
 Table
```

---

## Processing Pipeline

### 1. KafkaSource

- **Topic:** `{env}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session`
- **Consumer group:** `{env}.ads.stream-processing-engine`
- **Format:** Confluent Avro via `ShoppingEventDeserializationSchema`
- **Watermark strategy:** `forBoundedOutOfOrderness(Duration.ofMinutes(5))` — tolerates events up to 5 minutes late before considering them stale
- **Timestamp:** `event.getEventTimestampMs()` is used as the event-time timestamp for watermark assignment

### 2. Deduplication (`DeduplicationFunction`)

**Key:** `eventId` (`keyBy(ShoppingEvent::getEventId)`)

**Logic:**

```
On each event:
  IF seenState.value() == null:
    → record eventTimestampMs in state (with TTL)
    → forward event downstream
  ELSE:
    → silently drop (duplicate within TTL window)
```

**State:** `ValueState<Long>` keyed by `eventId`, with Flink-managed **StateTtlConfig**:
- TTL: `platform.dedup.ttl-minutes` (default: 60 minutes)
- Update type: `OnCreateAndWrite` — TTL resets on first write; duplicates within the window are always dropped
- Visibility: `NeverReturnExpired` — expired state is treated as absent
- Cleanup: `cleanupInRocksdbCompactFilter(1000)` — amortized compaction-based cleanup for large state

### 3. Attribution Join (`AttributionJoinFunction`)

**Key:** `sessionId` (`keyBy(ShoppingEvent::getSessionId)`)

**State:** Two `ValueState` entries per session, both with TTL aligned to `attributionWindowHours + 1`:
- `lastClick: ValueState<ShoppingEvent>` — the most recent `CLICK` event in this session
- `attributed: ValueState<Boolean>` — guard flag preventing double-attribution

**Logic:**

```
On CLICK event:
  → store event in lastClick state
  → clear attributed guard (allow next ADD_TO_CART to trigger attribution)
  → register processing-time timer at (now + attributionWindowHours)

On ADD_TO_CART event:
  → if lastClick is not null AND attributed guard is not set:
    → deltaMs = ADD_TO_CART.eventTimestampMs - lastClick.eventTimestampMs
    → if 0 ≤ deltaMs ≤ attributionWindowHours * 3,600,000:
      → set attributed guard (prevents double attribution for same click)
      → emit synthetic CLICK_TO_BASKET event:
          eventId:     cart.eventId + "_att"
          campaignId:  click.campaignId   ← attribution target
          eventType:   CLICK_TO_BASKET
          customTags:  {durationMs, attributedClickId}

On timer fire (attribution window expired):
  → clear lastClick state
  → clear attributed guard
```

**Attribution window:** `platform.attribution.window-hours` (default: 24 hours)

### 4. Sinks

| Sink | Class | Topic / Target | Data Written |
|:-----|:------|:---------------|:-------------|
| **Kafka enriched** | `KafkaSink<ShoppingEvent>` (Flink connector) | `{env}.internal.event.ads.attribution.ad-interaction-enriched-by-campaign` | All unique raw events + CLICK_TO_BASKET synthetics |
| **Redis hot counter** | `RedisHotCounterSink` | `campaign:{tenantId}:{campaignId}` hash | `HINCRBY` per eventType; TTL refresh 48 h |
| **Iceberg/S3** | `IcebergS3Sink` / `IcebergS3SinkStub` | S3 Parquet via Iceberg | All unique events (cold archive / source of truth) |
| **Kafka aggregate** | `KafkaAggregateSink` | `{env}.internal.event.ads.attribution...` (alternate path) | Pre-aggregated counts (Spring-Kafka fallback mode) |

#### Redis Hot-Counter Schema

```
Key:   campaign:{tenantId}:{campaignId}
Field: {eventType}  (e.g. CLICK, IMPRESSION, CLICK_TO_BASKET)
Value: counter (incremented atomically via HINCRBY)
TTL:   platform.flink.redis-ttl-seconds (default 172800 = 48 h, refreshed on every write)
```

---

## Package Structure

```
com.java.processing/
├── ProcessingApplication.java      # Spring Boot entry point

├── cleaner/
│   └── Deduplicator.java           # Lightweight in-memory dedup (Spring-Kafka fallback mode)

├── config/
│   ├── FlinkJobLauncher.java       # Daemon thread that calls FlinkStreamingJob.execute()
│   ├── FlinkProperties.java        # Binds platform.flink.* configuration
│   └── KafkaConfig.java            # Spring-Kafka consumer factory (fallback mode)

├── consumer/
│   └── EventConsumer.java          # Spring-Kafka listener (fallback when platform.flink.enabled=false)

├── job/
│   └── FlinkStreamingJob.java      # Full Flink DataStream topology definition and execution

├── join/
│   └── StatefulJoiner.java         # Reusable join logic (used by Spring-Kafka fallback)

├── observability/
│   └── ProcessingMetrics.java      # Micrometer: events_processed_total, dedup_dropped_total, etc.

├── operator/
│   ├── AttributionJoinFunction.java           # Flink KeyedProcessFunction: CLICK → CLICK_TO_BASKET
│   ├── DeduplicationFunction.java             # Flink KeyedProcessFunction: eventId dedup with TTL
│   ├── ShoppingEventDeserializationSchema.java # Flink KafkaRecordDeserializationSchema (Avro)
│   └── ShoppingEventSerializationSchema.java   # Flink KafkaRecordSerializationSchema (Avro)

└── sink/
    ├── IcebergS3Sink.java           # Flink RichSinkFunction: writes events to Iceberg table on S3
    ├── IcebergS3SinkStub.java       # No-op stub for local/dev when Iceberg is not configured
    ├── KafkaAggregateSink.java      # Writes pre-aggregated events back to Kafka (fallback mode)
    ├── PinotSink.java               # Direct Pinot REST sink (alternative to Kafka→Pinot path)
    └── RedisHotCounterSink.java     # Flink RichSinkFunction: Redis HINCRBY per event
```

---

## Configuration Reference

All properties are in `src/main/resources/application[-profile].yml`.

### Flink Job

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.flink.enabled` | `false` | Start the Flink DataStream job. `false` = use Spring-Kafka fallback consumer |
| `platform.flink.parallelism` | `1` | Default operator parallelism. Align with Kafka partition count in prod |
| `platform.flink.checkpoint-interval-ms` | `30000` | Checkpointing interval (0 = disabled) |
| `platform.flink.checkpoint-dir` | _(blank)_ | FileSystem URI for checkpoint storage (e.g. `s3://bucket/flink-checkpoints/`) |
| `platform.flink.use-rocks-db` | `false` | Use EmbeddedRocksDB incremental state backend (required for large state in prod) |
| `platform.flink.kafka-bootstrap-servers` | _(spring.kafka)_ | Override Kafka brokers for the Flink source/sink |
| `platform.flink.schema-registry-url` | `http://localhost:8081` | Confluent Schema Registry URL for Avro SerDes |

### Redis Sink (Flink)

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.flink.redis-host` | `localhost` | Redis host for the hot-counter sink |
| `platform.flink.redis-port` | `6379` | Redis port |
| `platform.flink.redis-password` | _(blank)_ | Redis password (blank = no auth) |
| `platform.flink.redis-database` | `0` | Redis database index |
| `platform.flink.redis-ttl-seconds` | `172800` | Campaign counter TTL (default 48 h) |

### Deduplication & Attribution

| Property | Default | Description |
|:---------|:--------|:------------|
| `platform.dedup.ttl-minutes` | `60` | EventId seen-state TTL for deduplication |
| `platform.attribution.window-hours` | `24` | Attribution window (CLICK → ADD_TO_CART max latency) |

### Kafka

| Property | Description |
|:---------|:------------|
| `platform.kafka.env` | Environment prefix for topic names (`local`, `dev`, `staging`, `prod`) |
| `platform.kafka.topic.raw` | Raw events input topic |
| `platform.kafka.topic.enriched` | Enriched events output topic |
| `platform.kafka.consumer-group.stream-engine` | Consumer group ID |

---

## State Management

The Flink job uses **two keyed state entries**:

| State | Operator | Key | Type | TTL | Purpose |
|:------|:---------|:----|:-----|:----|:--------|
| `dedup-seen-ts` | `DeduplicationFunction` | `eventId` | `ValueState<Long>` | 60 min | Record that this eventId was seen; auto-expired to bound state size |
| `last-click` | `AttributionJoinFunction` | `sessionId` | `ValueState<ShoppingEvent>` | `attributionWindow + 1h` | Hold the most recent CLICK for potential attribution |
| `attributed` | `AttributionJoinFunction` | `sessionId` | `ValueState<Boolean>` | `attributionWindow + 1h` | Guard flag preventing double attribution per session |

In **production**, the `EmbeddedRocksDBStateBackend` with incremental checkpoints is used (`platform.flink.use-rocks-db=true`). This allows state to grow beyond JVM heap for very high cardinality event streams (billions of unique sessions/eventIds) while maintaining fast local access.

In **local/dev**, the default in-memory backend is used (no RocksDB, no external checkpoint storage).

---

## Fault Tolerance & Checkpointing

| Setting | Value | Description |
|:--------|:------|:------------|
| Mode | `EXACTLY_ONCE` | Flink + Kafka transactional exactly-once delivery |
| Interval | `30 s` (configurable) | How often Flink takes a checkpoint |
| Timeout | `60 s` | Max time a checkpoint attempt may take |
| Min pause | `5 s` | Minimum gap between checkpoint completions |
| Max concurrent | `1` | Only one in-flight checkpoint at a time |
| Tolerable failures | `3` | Consecutive checkpoint failures before the job fails |
| External cleanup | `RETAIN_ON_CANCELLATION` | Checkpoint data retained when job is cancelled (allows restart recovery) |

**Kafka source offset commit:** Flink commits Kafka offsets as part of each checkpoint — if the job restarts from a checkpoint, events after the last checkpoint boundary are reprocessed (at-least-once on the Kafka consumer side; exactly-once achieved by the idempotent `DeduplicationFunction` downstream).

**Checkpoint storage:** Configurable via `platform.flink.checkpoint-dir` (e.g. `s3://my-bucket/flink-checkpoints/`). Leave blank for dev to use job-manager memory (not suitable for production).

---

## Deployment Modes

### Mode 1: Embedded Flink (Production)

`platform.flink.enabled=true`

The `FlinkJobLauncher` starts the `FlinkStreamingJob` in a daemon thread on application startup. The Spring Boot application serves as the container; the Flink mini-cluster runs inside the same JVM. This mode is suitable for self-managed Kubernetes deployments.

```yaml
platform:
  flink:
    enabled: true
    parallelism: 32        # match Kafka partition count
    use-rocks-db: true
    checkpoint-interval-ms: 30000
    checkpoint-dir: s3://my-bucket/flink-checkpoints/stream-engine
    redis-host: ${REDIS_HOST}
    redis-ttl-seconds: 172800
```

### Mode 2: Spring-Kafka Fallback (Local / Dev)

`platform.flink.enabled=false`

The `EventConsumer` Spring-Kafka listener processes events using a simple in-process pipeline: `Deduplicator` → `StatefulJoiner` → `KafkaAggregateSink`. This mode requires no Flink runtime and starts in seconds with just a Kafka broker.

```yaml
platform:
  flink:
    enabled: false
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

---

## Running Locally

**Prerequisites:** Java 21, Docker (Kafka + Redis via Docker Compose).

```bash
# 1. Start dependencies
cd deploy
docker-compose up -d kafka redis zookeeper

# 2. Create required Kafka topics
bash deploy/kafka/create-topics.sh

# 3. Run the engine (Spring-Kafka fallback mode)
cd stream-processing-engine
mvn spring-boot:run -Dspring-boot.run.profiles=local
# → Starts in seconds; no Flink runtime needed

# 4. (Optional) Run with embedded Flink
mvn spring-boot:run \
  -Dspring-boot.run.profiles=local \
  -Dspring-boot.run.jvmArguments="-Dplatform.flink.enabled=true"
```

---

## Building & Testing

```bash
# Compile
mvn -pl stream-processing-engine compile

# Run tests
mvn -pl stream-processing-engine test

# Fat JAR (includes embedded Flink runtime)
mvn -pl stream-processing-engine package -DskipTests

# Docker
docker build -t stream-processing-engine:latest stream-processing-engine/
```

### Test Coverage

| Test Class | Type | Covers |
|:-----------|:-----|:-------|
| `DeduplicatorTest` | Unit | First event passes through, duplicates are dropped, TTL expiry allows reprocessing |
| `StatefulJoinerTest` | Unit | CLICK → ADD_TO_CART attribution, double attribution guard, window expiry, non-attributable events |

---

## Key Design Decisions

| Decision | Rationale |
|:---------|:----------|
| **Dual-mode operation (Flink + Spring-Kafka fallback)** | Allows local development and integration testing without running a full Flink cluster while sharing the same codebase for production |
| **RocksDB incremental state backend** | Deduplication state for billions of unique `eventId` values per day cannot fit in JVM heap; incremental checkpoints minimize checkpoint overhead at scale |
| **Processing-time attribution timer** | Processing-time timers are simpler and more reliable than event-time timers for the attribution window — the window is a business policy (~24h) not a strict event-ordering constraint |
| **CLICK_TO_BASKET as a synthesized event** | Rather than enriching ADD_TO_CART events with click metadata, a synthetic event is emitted — this keeps the event schema simple and allows Pinot/Redis to count conversions as first-class metrics using the same counter pattern as clicks and impressions |
| **Redis TTL refresh on every write** | Active campaigns stay hot indefinitely without manual TTL management; inactive campaigns expire naturally after 48 hours, freeing memory |
| **`JedisPool` is transient in RedisHotCounterSink** | Flink serializes operators for distribution across TaskManagers; a `JedisPool` connection pool must not be serialized and must be created in `open()` per TaskManager |
| **At-least-once Kafka delivery for enriched topic** | The `DeduplicationFunction` downstream provides idempotency guarantees, so the slight overhead of exactly-once Kafka transactions on the sink is traded for simpler sink configuration (`AT_LEAST_ONCE` in `buildKafkaSink`) |
| **Watermark tolerance of 5 minutes** | Mobile app events frequently arrive late due to network queuing; 5-minute tolerance allows Flink's event-time windows to include most late events without unbounded waiting |

