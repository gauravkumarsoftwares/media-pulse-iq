# =====================================================================
# PROD deployment of the event-platform (full HA).
# Mirrors deploy/k8s/overlays/prod (namespace=prod, floors 10/3/5).
# =====================================================================
include "root" {
  path = find_in_parent_folders()
}

include "envcommon" {
  path           = "${dirname(find_in_parent_folders())}/_envcommon/event-platform.hcl"
  merge_strategy = "deep"
}

inputs = {
  # Pin to an immutable release tag in prod (set by CI on promotion).
  image_tag = "1.0.0"

  app_config = {
    SPRING_PROFILES_ACTIVE  = "prod"
    KAFKA_BOOTSTRAP_SERVERS = "kafka-prod-msk:9094"
    REDIS_HOST              = "redis-prod.cache"
    REDIS_PORT              = "6379"
    PINOT_BROKER            = "pinot-prod:8099"
    PINOT_CONTROLLER        = "pinot-prod:9000"
  }

  replica_floors = {
    ingestion = 10
    stream    = 3
    insights  = 5
  }

  scaling = {
    ingestion_max = 300
    insights_max  = 50
    stream_max    = 50
    kafka = {
      bootstrap_servers = "kafka-prod-msk:9094"
      consumer_group    = "group_stream_engine"
      topic             = "events.shopping.raw"
      lag_threshold     = "500000"
    }
  }
}

