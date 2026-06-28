# =====================================================================
# DEV environment-level config (inherited by all components in live/dev).
# =====================================================================
locals {
  environment  = "dev"
  aws_region   = "ap-south-1"
  # Cluster wiring - override via TG_* env vars or CI if needed.
  kubeconfig   = get_env("KUBECONFIG", "~/.kube/config")
  kube_context = "media-pulse-iq-dev"
}

