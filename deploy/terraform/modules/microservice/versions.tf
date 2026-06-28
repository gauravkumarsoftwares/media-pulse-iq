# =====================================================================
# microservice module - provider requirements
# =====================================================================
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30.0"
    }
    # Used for CRD-based resources (KEDA ScaledObject) so we don't hit the
    # kubernetes_manifest plan-time CRD-availability limitation.
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = ">= 1.14.0"
    }
  }
}

