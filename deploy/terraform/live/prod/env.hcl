# =====================================================================
# PROD environment-level config.
# =====================================================================
locals {
  environment  = "prod"
  aws_region   = "ap-south-1"
  kubeconfig   = get_env("KUBECONFIG", "~/.kube/config")
  kube_context = "media-pulse-iq-prod"
}

