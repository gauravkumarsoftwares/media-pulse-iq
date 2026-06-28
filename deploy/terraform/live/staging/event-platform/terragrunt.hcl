# =====================================================================
# STAGING deployment of the event-platform.
# Mirrors deploy/k8s/overlays/staging (namespace=staging).
# =====================================================================
include "root" {
  path = find_in_parent_folders()
}

include "envcommon" {
  path           = "${dirname(find_in_parent_folders())}/_envcommon/event-platform.hcl"
  merge_strategy = "deep"
}

inputs = {
  image_tag = "staging-latest"

  app_config = {
    SPRING_PROFILES_ACTIVE  = "staging"
    KAFKA_BOOTSTRAP_SERVERS = "kafka-staging:9092"
    REDIS_HOST              = "redis-staging"
    REDIS_PORT              = "6379"
    PINOT_BROKER            = "pinot-staging:8099"
    PINOT_CONTROLLER        = "pinot-staging:9000"
  }

  replica_floors = {
    ingestion = 3
    stream    = 2
    insights  = 2
  }

  scaling = {
    ingestion_max = 100
    insights_max  = 25
    stream_max    = 25
    kafka = {
      bootstrap_servers = "kafka-staging:9092"
      consumer_group    = "group_stream_engine"
      topic             = "events.shopping.raw"
      lag_threshold     = "250000"
    }
  }
}

