# =====================================================================
# microservice module - input variables
# A reusable building block for one Spring Boot service of the platform:
# Deployment + Service + (optional) HPA or KEDA ScaledObject.
# =====================================================================

variable "name" {
  description = "Service/Deployment name (e.g. ingestion-service)."
  type        = string
}

variable "namespace" {
  description = "Target Kubernetes namespace."
  type        = string
}

variable "image" {
  description = "Fully-qualified container image (repo:tag)."
  type        = string
}

variable "container_port" {
  description = "Port the container listens on."
  type        = number
}

variable "service_port" {
  description = "Port exposed by the ClusterIP Service."
  type        = number
  default     = 80
}

variable "replicas" {
  description = "Replica floor. Ignored after creation so HPA/KEDA owns scaling."
  type        = number
  default     = 1
}

variable "resources" {
  description = "CPU/memory requests and limits."
  type = object({
    requests = object({ cpu = string, memory = string })
    limits   = object({ cpu = string, memory = string })
  })
  default = {
    requests = { cpu = "500m", memory = "512Mi" }
    limits   = { cpu = "1", memory = "1Gi" }
  }
}

variable "config_map_ref" {
  description = "Name of a ConfigMap to inject as env (envFrom). Null to skip."
  type        = string
  default     = null
}

variable "secret_ref" {
  description = "Name of a Secret to inject as env (envFrom). Null to skip."
  type        = string
  default     = null
}

variable "readiness_path" {
  description = "HTTP readiness probe path."
  type        = string
  default     = "/actuator/health/readiness"
}

variable "liveness_path" {
  description = "HTTP liveness probe path."
  type        = string
  default     = "/actuator/health/liveness"
}

variable "readiness_initial_delay" {
  type    = number
  default = 15
}

variable "liveness_initial_delay" {
  type    = number
  default = 30
}

variable "common_labels" {
  description = "Labels merged onto every resource (e.g. part-of, environment)."
  type        = map(string)
  default     = {}
}

# ---------------------------------------------------------------------
# Autoscaling: choose "hpa" (CPU), "keda" (Kafka lag), or "none".
# ---------------------------------------------------------------------
variable "autoscaler" {
  description = "Autoscaling configuration for this service."
  type = object({
    type     = optional(string, "none") # "hpa" | "keda" | "none"
    min      = optional(number, 1)
    max      = optional(number, 10)
    cpu_target_utilization = optional(number, 75)
    # KEDA (Kafka) trigger settings - required when type == "keda".
    kafka = optional(object({
      bootstrap_servers = string
      consumer_group    = string
      topic             = string
      lag_threshold     = string
    }))
  })
  default = { type = "none" }

  validation {
    condition     = contains(["hpa", "keda", "none"], var.autoscaler.type)
    error_message = "autoscaler.type must be one of: hpa, keda, none."
  }

  validation {
    condition     = var.autoscaler.type != "keda" || var.autoscaler.kafka != null
    error_message = "autoscaler.kafka must be set when autoscaler.type is 'keda'."
  }
}

