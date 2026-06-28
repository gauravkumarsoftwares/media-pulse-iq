# =====================================================================
# _envcommon: shared definition of the "event-platform" component.
# Each env's live/<env>/event-platform/terragrunt.hcl includes this and
# only overrides the values that differ (image tag, scaling, endpoints).
# =====================================================================

locals {
  env_vars    = read_terragrunt_config(find_in_parent_folders("env.hcl"))
  environment = local.env_vars.locals.environment
}

# Point every env at the same versioned module.
# Point every env at the same versioned module.
terraform {
  source = "${get_repo_root()}/deploy/terraform//modules/event-platform"
}

# Baseline inputs; env-specific files merge their overrides on top.
inputs = {
  environment      = local.environment
  create_namespace = true
  image_registry   = "registry.io/media-pulse-iq"

  # Sensible platform-wide defaults; envs override scaling + app_config.
  app_secrets_name = "app-secrets"

  scaling = {
    ingestion_max = 300
    insights_max  = 50
    stream_max    = 50
  }
}

