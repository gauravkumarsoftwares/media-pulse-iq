# =====================================================================
# DEV deployment of the event-platform.
# Mirrors deploy/k8s/overlays/dev (namespace=dev, 2 replicas each).
# =====================================================================
include "root" {
  path = find_in_parent_folders()
}

include "envcommon" {
  path           = "${dirname(find_in_parent_folders())}/_envcommon/event-platform.hcl"
  merge_strategy = "deep"
}

inputs = {
  image_tag = "dev-latest"

  # Non-secret operational env -> 'app-config' ConfigMap.
  app_config = {
    SPRING_PROFILES_ACTIVE  = "dev"
    KAFKA_BOOTSTRAP_SERVERS = "kafka-dev:9092"
    REDIS_HOST              = "redis-dev"
    REDIS_PORT              = "6379"
    PINOT_BROKER            = "pinot-dev:8099"
    PINOT_CONTROLLER        = "pinot-dev:9000"
  }

  replica_floors = {
    ingestion = 2
    stream    = 2
    insights  = 2
  }

  # Smaller ceilings for the cost-controlled dev cluster.
  scaling = {
    ingestion_max = 20
    insights_max  = 10
    stream_max    = 10
    kafka = {
      bootstrap_servers = "kafka-dev:9092"
      consumer_group    = "group_stream_engine"
      topic             = "events.shopping.raw"
      lag_threshold     = "100000"
    }
  }
}

