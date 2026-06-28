# =====================================================================
# STAGING environment-level config.
# =====================================================================
locals {
  environment  = "staging"
  aws_region   = "ap-south-1"
  kubeconfig   = get_env("KUBECONFIG", "~/.kube/config")
  kube_context = "media-pulse-iq-staging"
}

