# =====================================================================
# event-platform module - outputs
# =====================================================================
output "namespace" {
  description = "Namespace the platform was deployed into."
  value       = local.namespace
}

output "services" {
  description = "In-cluster DNS + port for each deployed service."
  value = {
    ingestion = {
      dns  = module.ingestion_service.service_dns
      port = module.ingestion_service.service_port
    }
    stream = {
      dns  = module.stream_processing_engine.service_dns
      port = module.stream_processing_engine.service_port
    }
    insights = {
      dns  = module.insights_query_service.service_dns
      port = module.insights_query_service.service_port
    }
  }
}

