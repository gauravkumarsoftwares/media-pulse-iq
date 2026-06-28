# =====================================================================
# event-platform module - namespace, ConfigMap, and the 3 microservices.
# =====================================================================

locals {
  namespace = coalesce(var.namespace, var.environment)

  common_labels = merge(
    {
      "app.kubernetes.io/part-of" = "media-pulse-iq"
      "app.kubernetes.io/managed-by" = "terraform"
      "environment"               = var.environment
    },
    var.common_labels
  )

  # Default KEDA Kafka trigger if the caller didn't pass one explicitly.
  kafka_trigger = coalesce(try(var.scaling.kafka, null), {
    bootstrap_servers = lookup(var.app_config, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer_group    = "group_stream_engine"
    topic             = "events.shopping.raw"
    lag_threshold     = "500000"
  })
}

# ----- Namespace -----------------------------------------------------
resource "kubernetes_namespace_v1" "this" {
  count = var.create_namespace ? 1 : 0

  metadata {
    name   = local.namespace
    labels = local.common_labels
  }
}

# ----- app-config ConfigMap (non-secret operational env) -------------
resource "kubernetes_config_map_v1" "app_config" {
  metadata {
    name      = "app-config"
    namespace = local.namespace
    labels    = local.common_labels
  }

  data = var.app_config

  depends_on = [kubernetes_namespace_v1.this]
}

# ----- ingestion-service (Write Path, HPA on CPU) --------------------
module "ingestion_service" {
  source = "../microservice"

  name           = "ingestion-service"
  namespace      = local.namespace
  image          = "${var.image_registry}/ingestion-service:${var.image_tag}"
  container_port = 8080
  replicas       = try(var.replica_floors.ingestion, 1)
  config_map_ref = kubernetes_config_map_v1.app_config.metadata[0].name
  secret_ref     = var.app_secrets_name
  common_labels  = local.common_labels

  resources = {
    requests = { cpu = "500m", memory = "512Mi" }
    limits   = { cpu = "1", memory = "1Gi" }
  }

  autoscaler = {
    type                   = "hpa"
    min                    = try(var.replica_floors.ingestion, 1)
    max                    = var.scaling.ingestion_max
    cpu_target_utilization = 75
  }

  depends_on = [kubernetes_namespace_v1.this]
}

# ----- stream-processing-engine (Stream Path, KEDA on Kafka lag) -----
module "stream_processing_engine" {
  source = "../microservice"

  name           = "stream-processing-engine"
  namespace      = local.namespace
  image          = "${var.image_registry}/stream-processing-engine:${var.image_tag}"
  container_port = 8082
  replicas       = try(var.replica_floors.stream, 1)
  config_map_ref = kubernetes_config_map_v1.app_config.metadata[0].name
  secret_ref     = var.app_secrets_name
  common_labels  = local.common_labels

  resources = {
    requests = { cpu = "1", memory = "1Gi" }
    limits   = { cpu = "2", memory = "2Gi" }
  }

  readiness_initial_delay = 20
  liveness_initial_delay  = 40

  autoscaler = {
    type  = "keda"
    min   = try(var.replica_floors.stream, 1)
    max   = var.scaling.stream_max
    kafka = local.kafka_trigger
  }

  depends_on = [kubernetes_namespace_v1.this]
}

# ----- insights-query-service (Read Path / CQRS, HPA on CPU) ---------
module "insights_query_service" {
  source = "../microservice"

  name           = "insights-query-service"
  namespace      = local.namespace
  image          = "${var.image_registry}/insights-query-service:${var.image_tag}"
  container_port = 8083
  replicas       = try(var.replica_floors.insights, 1)
  config_map_ref = kubernetes_config_map_v1.app_config.metadata[0].name
  secret_ref     = var.app_secrets_name
  common_labels  = local.common_labels

  resources = {
    requests = { cpu = "500m", memory = "512Mi" }
    limits   = { cpu = "1", memory = "1Gi" }
  }

  autoscaler = {
    type                   = "hpa"
    min                    = try(var.replica_floors.insights, 1)
    max                    = var.scaling.insights_max
    cpu_target_utilization = 70
  }

  depends_on = [kubernetes_namespace_v1.this]
}

