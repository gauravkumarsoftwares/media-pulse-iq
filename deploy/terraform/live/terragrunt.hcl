# =====================================================================
# ROOT Terragrunt configuration (applies to every env under live/).
# Provides:
#   - DRY remote state (S3 + DynamoDB lock) auto-keyed per child path
#   - Auto-generated kubernetes + kubectl providers wired to the env's
#     cluster (context + kubeconfig come from each env.hcl)
# =====================================================================

# ----- Locals pulled from the env-level config (live/<env>/env.hcl) ---
locals {
  env_vars    = read_terragrunt_config(find_in_parent_folders("env.hcl"))
  environment = local.env_vars.locals.environment
  aws_region  = local.env_vars.locals.aws_region
  kube_context = local.env_vars.locals.kube_context
  kubeconfig   = local.env_vars.locals.kubeconfig
}

# ----- Remote state: one bucket, key namespaced by path ---------------
remote_state {
  backend = "s3"

  generate = {
    path      = "backend.tf"
    if_exists = "overwrite_terragrunt"
  }

  config = {
    bucket         = "media-pulse-iq-tfstate-${local.environment}"
    key            = "${path_relative_to_include()}/terraform.tfstate"
    region         = local.aws_region
    encrypt        = true
    dynamodb_table = "media-pulse-iq-tflock-${local.environment}"
  }
}

# ----- Provider generation (Kubernetes + kubectl) ---------------------
generate "providers" {
  path      = "providers.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    provider "kubernetes" {
      config_path    = "${local.kubeconfig}"
      config_context = "${local.kube_context}"
    }

    provider "kubectl" {
      config_path      = "${local.kubeconfig}"
      config_context   = "${local.kube_context}"
      load_config_file = true
    }
  EOF
}

# ----- Pin Terraform + provider versions for every child --------------
generate "versions" {
  path      = "versions_generated.tf"
  if_exists = "overwrite_terragrunt"
  contents  = <<-EOF
    terraform {
      required_version = ">= 1.6.0"
      required_providers {
        kubernetes = {
          source  = "hashicorp/kubernetes"
          version = ">= 2.30.0"
        }
        kubectl = {
          source  = "gavinbunney/kubectl"
          version = ">= 1.14.0"
        }
      }
    }
  EOF
}

# Inputs every child inherits.
inputs = {
  environment = local.environment
}

