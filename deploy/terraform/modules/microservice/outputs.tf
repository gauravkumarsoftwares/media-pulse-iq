# =====================================================================
# microservice module - outputs
# =====================================================================
output "name" {
  description = "Deployment/Service name."
  value       = kubernetes_deployment_v1.this.metadata[0].name
}

output "service_dns" {
  description = "In-cluster DNS name of the Service."
  value       = "${kubernetes_service_v1.this.metadata[0].name}.${var.namespace}.svc.cluster.local"
}

output "service_port" {
  description = "ClusterIP Service port."
  value       = var.service_port
}

