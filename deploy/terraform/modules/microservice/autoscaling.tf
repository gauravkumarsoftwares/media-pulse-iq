# =====================================================================
# microservice module - autoscaling
#   type = "hpa"  -> CPU-based HorizontalPodAutoscaler (ingestion / insights)
#   type = "keda" -> Kafka-lag ScaledObject           (stream engine)
#   type = "none" -> no autoscaler
# =====================================================================

# ----- HPA (CPU utilisation) -----------------------------------------
resource "kubernetes_horizontal_pod_autoscaler_v2" "this" {
  count = var.autoscaler.type == "hpa" ? 1 : 0

  metadata {
    name      = "${var.name}-hpa"
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = kubernetes_deployment_v1.this.metadata[0].name
    }

    min_replicas = var.autoscaler.min
    max_replicas = var.autoscaler.max

    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = var.autoscaler.cpu_target_utilization
        }
      }
    }
  }
}

# ----- KEDA ScaledObject (Kafka consumer lag) ------------------------
# Applied via kubectl_manifest so we don't depend on the KEDA CRD being
# registered at plan time (kubernetes_manifest limitation).
resource "kubectl_manifest" "keda_scaledobject" {
  count = var.autoscaler.type == "keda" ? 1 : 0

  yaml_body = yamlencode({
    apiVersion = "keda.sh/v1alpha1"
    kind       = "ScaledObject"
    metadata = {
      name      = "${var.name}-scaler"
      namespace = var.namespace
      labels    = local.labels
    }
    spec = {
      scaleTargetRef = {
        name = kubernetes_deployment_v1.this.metadata[0].name
      }
      minReplicaCount = var.autoscaler.min
      maxReplicaCount = var.autoscaler.max
      triggers = [
        {
          type = "kafka"
          metadata = {
            bootstrapServers = var.autoscaler.kafka.bootstrap_servers
            consumerGroup    = var.autoscaler.kafka.consumer_group
            topic            = var.autoscaler.kafka.topic
            lagThreshold     = var.autoscaler.kafka.lag_threshold
          }
        }
      ]
    }
  })
}

