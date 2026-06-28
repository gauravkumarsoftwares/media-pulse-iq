# =====================================================================
# event-platform module - input variables
# Composes the full streaming-insight platform for a single environment:
#   namespace + app-config ConfigMap + 3 microservices (with autoscalers).
# =====================================================================

variable "environment" {
  description = "Environment name (dev | staging | prod). Drives the namespace."
  type        = string
}

variable "namespace" {
  description = "Override the target namespace. Defaults to var.environment."
  type        = string
  default     = null
}

variable "create_namespace" {
  description = "Whether this module should create the namespace."
  type        = bool
  default     = true
}

variable "image_registry" {
  description = "Container image registry/prefix."
  type        = string
  default     = "registry.io/media-pulse-iq"
}

variable "image_tag" {
  description = "Image tag deployed across all services (e.g. git SHA or version)."
  type        = string
  default     = "latest"
}

# Non-secret operational config injected into all pods as an env ConfigMap.
# Mirrors deploy/k8s/overlays/<env>/configmap.env.
variable "app_config" {
  description = "Key/value map rendered into the 'app-config' ConfigMap."
  type        = map(string)
}

# Name of an externally-managed Secret (External Secrets Operator / Vault)
# providing credentials + PASETO_PUBLIC_KEY. Not created by this module.
variable "app_secrets_name" {
  description = "Secret name injected as env into pods (envFrom). Null to skip."
  type        = string
  default     = "app-secrets"
}

# Per-service replica floors (HPA/KEDA scale above these).
variable "replica_floors" {
  description = "Minimum replicas per service."
  type = object({
    ingestion = optional(number, 1)
    stream    = optional(number, 1)
    insights  = optional(number, 1)
  })
  default = {}
}

# Autoscaling ceilings + Kafka trigger wiring.
variable "scaling" {
  description = "Autoscaling bounds for ingestion (HPA), insights (HPA), stream (KEDA)."
  type = object({
    ingestion_max = optional(number, 300)
    insights_max  = optional(number, 50)
    stream_max    = optional(number, 50)
    kafka = optional(object({
      bootstrap_servers = string
      consumer_group    = string
      topic             = string
      lag_threshold     = string
    }))
  })
}

variable "common_labels" {
  description = "Extra labels merged onto every resource."
  type        = map(string)
  default     = {}
}

