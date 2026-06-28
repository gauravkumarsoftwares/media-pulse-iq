# =====================================================================
# microservice module - Deployment + Service
# Mirrors deploy/k8s/base/*.yaml but parameterised & multi-env aware.
# =====================================================================

locals {
  labels = merge({ app = var.name }, var.common_labels)
}

resource "kubernetes_deployment_v1" "this" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    # Replica floor only. HPA/KEDA manage the live count (see lifecycle).
    replicas = var.replicas

    selector {
      match_labels = { app = var.name }
    }

    template {
      metadata {
        labels = local.labels
      }

      spec {
        container {
          name  = var.name
          image = var.image

          port {
            container_port = var.container_port
          }

          # envFrom ConfigMap (operational, non-secret values)
          dynamic "env_from" {
            for_each = var.config_map_ref == null ? [] : [var.config_map_ref]
            content {
              config_map_ref {
                name = env_from.value
              }
            }
          }

          # envFrom Secret (credentials, PASETO_PUBLIC_KEY, ...)
          dynamic "env_from" {
            for_each = var.secret_ref == null ? [] : [var.secret_ref]
            content {
              secret_ref {
                name = env_from.value
              }
            }
          }

          resources {
            requests = {
              cpu    = var.resources.requests.cpu
              memory = var.resources.requests.memory
            }
            limits = {
              cpu    = var.resources.limits.cpu
              memory = var.resources.limits.memory
            }
          }

          readiness_probe {
            http_get {
              path = var.readiness_path
              port = var.container_port
            }
            initial_delay_seconds = var.readiness_initial_delay
          }

          liveness_probe {
            http_get {
              path = var.liveness_path
              port = var.container_port
            }
            initial_delay_seconds = var.liveness_initial_delay
          }
        }
      }
    }
  }

  # The replica count is owned by HPA/KEDA at runtime; don't let Terraform
  # fight the autoscaler on subsequent applies.
  lifecycle {
    ignore_changes = [spec[0].replicas]
  }
}

resource "kubernetes_service_v1" "this" {
  metadata {
    name      = var.name
    namespace = var.namespace
    labels    = local.labels
  }

  spec {
    selector = { app = var.name }

    port {
      port        = var.service_port
      target_port = var.container_port
    }
  }
}

