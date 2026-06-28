#!/usr/bin/env bash
# =============================================================================
# Kafka Topic Provisioning & Retention Policy Script
# =============================================================================
# Creates all platform topics with appropriate retention, replication, and
# partition counts aligned with the architecture (2.2).
#
# Usage:
#   KAFKA_ENV=prod KAFKA_BROKER=kafka:9092 KAFKA_PARTITIONS=128 ./create-topics.sh
#
# Defaults are for local Docker Compose (1 broker, 1 partition).
# =============================================================================

set -euo pipefail

BROKER="${KAFKA_BROKER:-localhost:9092}"
ENV="${KAFKA_ENV:-local}"
PARTITIONS="${KAFKA_PARTITIONS:-1}"
REPLICATION="${KAFKA_REPLICATION:-1}"   # set to 3 in staging/prod

echo "Creating topics for env=${ENV} broker=${BROKER} partitions=${PARTITIONS} rf=${REPLICATION}"

kafka-topics.sh --bootstrap-server "${BROKER}" --create --if-not-exists \
  --topic "${ENV}.shared.event.ads.clickstream.ad-interaction-received-by-tenant-session" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}" \
  --config retention.ms=86400000 \        # 24 h — Flink reprocesses from checkpoints, not from Kafka re-reads
  --config segment.bytes=536870912 \      # 512 MB segment files
  --config min.insync.replicas=2 \        # ack from 2 replicas before producer ack (staging/prod)
  --config compression.type=lz4           # LZ4: fast compression; reduces network/disk I/O ~40%

kafka-topics.sh --bootstrap-server "${BROKER}" --create --if-not-exists \
  --topic "${ENV}.internal.event.ads.clickstream.ad-interaction-failed-by-tenant-id" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}" \
  --config retention.ms=604800000 \       # 7 days DLQ — ops team has time to triage + replay
  --config segment.bytes=268435456 \      # 256 MB
  --config compression.type=gzip          # GZIP: better ratio for infrequent DLQ traffic

kafka-topics.sh --bootstrap-server "${BROKER}" --create --if-not-exists \
  --topic "${ENV}.internal.event.ads.attribution.ad-interaction-enriched-by-campaign" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}" \
  --config retention.ms=3600000 \         # 1 h — Pinot real-time tables consume immediately;
                                          # insight-query-service is the long-term store
  --config segment.bytes=536870912 \
  --config min.insync.replicas=2 \
  --config compression.type=lz4

echo "All topics created successfully."

# ---- Kafka quotas for noisy-neighbour protection (6.2) ----
# Set producer/consumer byte-rate limits per tenant client-id.
# These are configured via kafka-configs.sh at the principal (user) level;
# example for a standard-tier tenant:
#
#   kafka-configs.sh --bootstrap-server "${BROKER}" --alter --add-config \
#     'producer_byte_rate=10485760,consumer_byte_rate=20971520' \
#     --entity-type users --entity-name walmart_us_producer
#
# Enterprise tiers: remove the quota (unlimited / dynamic autoscaling).

echo "Reminder: set Kafka producer/consumer quotas per tenant principal (6.2)."

