# shared-model

> **Role in the platform:** Shared internal library that defines the canonical data model, Avro serialization/deserialization, Redis key naming conventions, and Kafka topic naming conventions used across all services (`ingestion-service`, `stream-processing-engine`, `insights-query-service`).

---

## Table of Contents

- [Purpose](#purpose)
- [Package Structure](#package-structure)
- [Core Domain Model](#core-domain-model)
  - [ShoppingEvent](#shoppingevent)
  - [EventType](#eventtype)
- [Avro Serialization](#avro-serialization)
  - [ShoppingEventAvroSerde](#shoppingeventavroserde)
  - [Confluent SerDes](#confluent-serdes)
- [Redis Key Schema](#redis-key-schema)
- [Kafka Topic Conventions](#kafka-topic-conventions)
  - [Naming Convention](#naming-convention)
  - [Topic Catalogue](#topic-catalogue)
  - [Consumer-Group Catalogue](#consumer-group-catalogue)
- [Adding to Your Module](#adding-to-your-module)
- [Building](#building)
- [Design Principles](#design-principles)

---

## Purpose

`shared-model` is the single source of truth for:

1. **The `ShoppingEvent` domain object** — the unit of data flowing through the entire platform pipeline, from the HTTP ingest endpoint through Kafka, Flink processing, Pinot storage, and the query serving layer.
2. **Avro wire format** — Confluent Schema Registry–compatible serializers and deserializers so every producer and consumer is guaranteed schema compatibility.
3. **Redis key schema** — centralized key and field naming patterns so the Flink write path (`stream-processing-engine → RedisHotCounterSink`) and the Spring read path (`insights-query-service → RedisInsightsStore`) always agree on key structure.
4. **Kafka topic naming** — centralized naming conventions and template constants prevent topic-name drift across services.

By publishing this as a shared library, the platform enforces a strict contract boundary: any change to `ShoppingEvent` is a deliberate, compile-breaking contract change that must be versioned.

---

## Package Structure

```
com.java.model/
├── ShoppingEvent.java             # Core domain object (Lombok @Builder, Kafka + Flink wire format)
├── EventType.java                 # Enum of all recognized event types + null-safe parser
│
├── avro/
│   ├── ShoppingEventAvroSerde.java               # Low-level Avro GenericRecord ↔ ShoppingEvent mapper
│   ├── ShoppingEventConfluentDeserializer.java   # Confluent KafkaAvroDeserializer wrapper
│   └── ShoppingEventConfluentSerializer.java     # Confluent KafkaAvroSerializer wrapper
│
├── constants/
│   └── KafkaTopics.java           # Topic/consumer-group name templates + resolve() utility
│
└── redis/
    └── RedisKeySchema.java        # Canonical Redis key/field patterns for campaign counters + time-series
```

---

## Core Domain Model

### ShoppingEvent

The fundamental unit of data in the platform. Represents a single ad-interaction event (click, impression, add-to-cart, etc.) from a retailer's digital storefront.

```java
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ShoppingEvent implements Serializable {

    String eventId;          // Unique identifier — used for Flink deduplication keying
    String tenantId;         // Retailer identifier — all data queries are scoped to this
    String userId;           // End-user identifier (pseudonymized before logging)
    String sessionId;        // Browser/app session — used for Flink attribution joins (CLICK → ADD_TO_CART)
    String campaignId;       // Ad campaign this event is attributed to
    String eventType;        // One of EventType enum values (stored as String for Kafka/Avro compat)
    long   eventTimestampMs; // Client-side event timestamp in epoch milliseconds
    double cost;             // Ad cost in USD at the time of the event
    Map<String, String> customTags; // Arbitrary retailer-specific metadata
}
```

**Field usage across the pipeline:**

| Field | Ingestion | Flink | Redis | Pinot | Query |
|:------|:---------:|:-----:|:-----:|:-----:|:-----:|
| `eventId` | PK for validation | Dedup key (`keyBy`) | — | partition key | — |
| `tenantId` | auth source | preserved | hash-key segment | partition key + filter | RLS predicate |
| `sessionId` | validated | attribution join key | — | — | — |
| `campaignId` | optional | attribution output | hash-key segment | filter | path variable |
| `eventType` | schema-validated | routes to sinks | hash-field | event_type column | metric type |
| `eventTimestampMs` | defaulted to server time if 0 | watermark source | time-series bucket | time-series bucketing | `from`/`to` window |

### EventType

```java
public enum EventType {
    UNSPECIFIED,        // Default / unknown — causes schema validation failure in ingestion
    IMPRESSION,         // Ad was displayed to the user
    CLICK,              // User clicked on the ad
    PRODUCT_VIEW,       // User viewed a product page
    ADD_TO_CART,        // User added a product to their cart
    PURCHASE,           // User completed a purchase
    CLICK_TO_BASKET     // Synthesized by Flink: CLICK → ADD_TO_CART attribution within the window
}
```

**Key method:**

```java
// Null-safe, case-insensitive parse. Unknown values return UNSPECIFIED.
EventType type = EventType.from("click");   // → EventType.CLICK
EventType bad  = EventType.from("UNKNOWN"); // → EventType.UNSPECIFIED
```

`CLICK_TO_BASKET` events are **never produced by clients** — they are synthesized by the `AttributionJoinFunction` in `stream-processing-engine` when a `CLICK` is followed by an `ADD_TO_CART` within the attribution window (default 24 hours, same session).

---

## Avro Serialization

All events are serialized on Kafka using **Apache Avro** with the **Confluent Schema Registry** wire format (magic byte + schema ID prefix).

### Schema Definition

The Avro schema is defined programmatically in `ShoppingEventAvroSerde` as a `Schema.Parser` JSON string, mirroring the `ShoppingEvent` Java fields:

```
{
  "type": "record",
  "name": "ShoppingEvent",
  "namespace": "com.java.model",
  "fields": [
    {"name": "eventId",          "type": ["null", "string"], "default": null},
    {"name": "tenantId",         "type": ["null", "string"], "default": null},
    {"name": "userId",           "type": ["null", "string"], "default": null},
    {"name": "sessionId",        "type": ["null", "string"], "default": null},
    {"name": "campaignId",       "type": ["null", "string"], "default": null},
    {"name": "eventType",        "type": ["null", "string"], "default": null},
    {"name": "eventTimestampMs", "type": "long",              "default": 0},
    {"name": "cost",             "type": "double",            "default": 0.0},
    {"name": "customTags",       "type": ["null", {"type": "map", "values": "string"}], "default": null}
  ]
}
```

All string fields use the `["null", "string"]` union type with a `null` default to support forward-compatible schema evolution (new optional fields can be added without breaking existing consumers).

### ShoppingEventAvroSerde

Low-level converter between `ShoppingEvent` Java POJOs and Avro `GenericRecord`:

```java
// Serialize ShoppingEvent → GenericRecord
GenericRecord record = ShoppingEventAvroSerde.toAvro(event);

// Deserialize GenericRecord → ShoppingEvent
ShoppingEvent event = ShoppingEventAvroSerde.fromAvro(record);
```

Used internally by the Confluent SerDes wrappers.

### Confluent SerDes

**`ShoppingEventConfluentSerializer`** — wraps `KafkaAvroSerializer`. Registered as the Kafka producer value serializer:

```yaml
spring:
  kafka:
    producer:
      value-serializer: com.java.model.avro.ShoppingEventConfluentSerializer
```

**`ShoppingEventConfluentDeserializer`** — wraps `KafkaAvroDeserializer`. Registered as the Kafka consumer value deserializer:

```yaml
spring:
  kafka:
    consumer:
      value-deserializer: com.java.model.avro.ShoppingEventConfluentDeserializer
```

Both classes delegate to the Confluent `KafkaAvroSerializer` / `KafkaAvroDeserializer` and handle the Schema Registry interaction automatically.

**Schema Registry configuration:**

```yaml
platform:
  schema-registry:
    url: http://localhost:8081   # local Docker Compose
    # Production: https://psrc-xxxxx.us-east-2.aws.confluent.cloud
```

---

## Redis Key Schema

`RedisKeySchema` is the single source of truth for all Redis key and field naming patterns used across the platform. It prevents the write path (`RedisHotCounterSink` in `stream-processing-engine`) and the read path (`RedisInsightsStore` in `insights-query-service`) from drifting to different key formats independently.

### Key Patterns

```
Aggregate hash key  :  campaign:{tenantId}:{campaignId}
Hash field          :  {eventType}   (CLICK, IMPRESSION, CLICK_TO_BASKET)
Value               :  counter (HINCRBY — total aggregate)

Time-series hash key  :  ts:{tenantId}:{campaignId}:{eventType}
Hash field            :  {hourBucket_epoch_ms}  (hour-aligned, milliseconds)
Value                 :  counter per bucket (HINCRBY)
```

### API

```java
// Aggregate hash key for campaign counters
String key = RedisKeySchema.hashKey("walmart_us", "cmp_spring_99a");
// → "campaign:walmart_us:cmp_spring_99a"

// Time-series hash key — one key per (tenant, campaign, eventType)
String tsKey = RedisKeySchema.timeSeriesKey("walmart_us", "cmp_spring_99a", "CLICK");
// → "ts:walmart_us:cmp_spring_99a:CLICK"

// Truncate event timestamp to the start of its UTC hour (hash field for time-series)
long bucket = RedisKeySchema.hourBucket(1750500000000L);
// → epoch ms of the hour-aligned boundary
```

---

## Kafka Topic Conventions

### Naming Convention

All topic names follow a structured template that encodes visibility, type, domain context, and partition key:

```
{env}.{visibility}.{topic-type}.{domain}.{subdomain}.{record-name}-by-{key-name}[-v{N}]
```

| Segment | Values | Notes |
|:--------|:-------|:------|
| `{env}` | `local`, `dev`, `staging`, `prod` | Set via `KAFKA_ENV` env-var |
| `{visibility}` | `external`, `shared`, `internal`, `private` | Cross-domain scope |
| `{topic-type}` | `event`, `command`, `entity`, `cdc`, `notification` | DDD event type |
| `{domain}.{subdomain}` | e.g. `ads.clickstream` | Bounded-context hierarchy |
| `{record-name}` | Noun + PastTense for events | e.g. `ad-interaction-received` |
| `{key-name}` | The Kafka partition key field | e.g. `by-tenant-session` |
| `[-v{N}]` | Optional version suffix | Increment on incompatible schema change |

> ⚠️ **Never hardcode topic names in application code.** Use Spring property placeholders (`${platform.kafka.topic.raw}`) — the `{env}` segment is injected at runtime via `KAFKA_ENV`.

### Topic Catalogue

| Template Constant | Resolved Example (prod) | Direction | Key |
|:------------------|:------------------------|:----------|:----|
| `TEMPLATE_RAW` | `prod.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session` | ingestion-service → stream-processing-engine | `tenantId:sessionId` |
| `TEMPLATE_DLQ` | `prod.internal.event.ads.clickstream.ad-interaction-failed-by-tenant-id` | ingestion-service → SRE tooling | `tenantId` |
| `TEMPLATE_ENRICHED` | `prod.internal.event.ads.attribution.ad-interaction-enriched-by-campaign` | stream-processing-engine → Pinot + insights-query-service | `tenantId:campaignId` |
| `TEMPLATE_RECONCILIATION_CORRECTIONS` | `prod.internal.command.ads.reconciliation.counter-correction-by-campaign` | reconciliation-job → SRE replay tooling | `tenantId:campaignId` |

**Resolve a template at runtime:**

```java
String topic = KafkaTopics.resolve(KafkaTopics.TEMPLATE_RAW, "prod");
// → "prod.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session"
```

### Consumer-Group Catalogue

| Template Constant | Resolved Example (prod) | Consumer |
|:------------------|:------------------------|:---------|
| `TEMPLATE_GROUP_STREAM_ENGINE` | `prod.ads.stream-processing-engine` | Flink job / Spring-Kafka fallback |
| `TEMPLATE_GROUP_INSIGHTS_SERVING` | `prod.ads.insights-query-service` | `AggregateConsumer` in insights-query-service |

---

## Adding to Your Module

Add the Maven dependency (version is managed by the parent BOM):

```xml
<dependency>
    <groupId>com.java</groupId>
    <artifactId>shared-model</artifactId>
</dependency>
```

All transitive dependencies (Avro, Confluent serializers, Jackson annotations) are included.

---

## Building

```bash
# Compile and install to local Maven repository
mvn -pl shared-model install -DskipTests

# From the root (builds all modules in dependency order)
mvn install -DskipTests
```

`shared-model` has **no tests** — correctness is covered by integration tests in the consumer modules.

---

## Design Principles

| Principle | Application |
|:----------|:------------|
| **Single Source of Truth** | One `ShoppingEvent` class, one Avro schema, one `KafkaTopics` catalogue, one `RedisKeySchema` — no duplication across modules |
| **Forward-compatible Avro schema** | All string fields use `["null", "string"]` union with `null` default — new optional fields can be added without breaking existing consumers |
| **No Spring dependencies** | `shared-model` is a plain Java library. It does not import Spring Boot; any module can use it regardless of framework |
| **String-typed `eventType` on the wire** | Using `String` instead of the `EventType` enum allows unknown future event types to pass through the Kafka/Avro layer; type safety is enforced at the application level by `EventType.from()` |
| **Immutable `EventType.from()`** | Null-safe, case-insensitive, never throws — maps unknown strings to `UNSPECIFIED` so the caller can decide how to handle them |
| **`RedisKeySchema` as DRY boundary** | Key-building logic is shared so that the Flink sink and the Spring read service are always structurally compatible — a key format change is a compile-visible contract change |
