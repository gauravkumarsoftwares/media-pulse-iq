# Terraform + Terragrunt deployment for the Streaming Insight Platform

Infrastructure-as-Code that deploys the three platform services
(`ingestion-service`, `stream-processing-engine`, `insights-query-service`)
to Kubernetes, with per-environment configuration driven by **Terragrunt**.

This is the IaC equivalent of `deploy/k8s/` (Kustomize base + overlays) — same
resources, same per-env values, but managed via Terraform state.

## Layout

```
deploy/terraform/
├── modules/
│   ├── microservice/          # Reusable: Deployment + Service + HPA|KEDA
│   └── event-platform/        # Composition: namespace + ConfigMap + 3 services
└── live/                      # Terragrunt orchestration (DRY, per-env)
    ├── terragrunt.hcl         # Root: remote state + generated providers
    ├── _envcommon/
    │   └── event-platform.hcl # Shared component definition
    ├── dev/
    │   ├── env.hcl            # Cluster context, region, env name
    │   └── event-platform/terragrunt.hcl
    ├── staging/ …
    └── prod/ …
```

## What gets created (per environment)

| Resource | Source | Notes |
| :-- | :-- | :-- |
| `Namespace` | event-platform | `dev` / `staging` / `prod` |
| `ConfigMap/app-config` | event-platform | Non-secret env (Kafka/Redis/Pinot endpoints) |
| `Deployment` + `Service` ×3 | microservice | One per platform service |
| `HorizontalPodAutoscaler` ×2 | microservice | ingestion (max 300), insights (max 50) on CPU |
| `ScaledObject` (KEDA) ×1 | microservice | stream engine on Kafka consumer lag |

> **Secrets are NOT managed here.** Credentials + `PASETO_PUBLIC_KEY` come from
> a Secret named `app-secrets`, synced by the External Secrets Operator / Vault,
> and injected via `envFrom`. The Deployments reference it but never create it.

## Environment matrix (mirrors the Kustomize overlays)

| Env | Replica floors (ingest/stream/insights) | Scaling ceilings | Image tag |
| :-- | :-- | :-- | :-- |
| dev | 2 / 2 / 2 | 20 / 10 / 10 | `dev-latest` |
| staging | 3 / 2 / 2 | 100 / 25 / 25 | `staging-latest` |
| prod | 10 / 3 / 5 | 300 / 50 / 50 | `1.0.0` |

## Prerequisites

- Terraform >= 1.6, Terragrunt >= 0.55
- A reachable cluster + kube-context per env (`media-pulse-iq-<env>`)
- [KEDA](https://keda.sh) installed in-cluster (provides the `ScaledObject` CRD)
- (For remote state) an S3 bucket `media-pulse-iq-tfstate-<env>` and DynamoDB
  lock table `media-pulse-iq-tflock-<env>`, or edit `live/terragrunt.hcl`.

## Usage

```bash
# Plan/apply a single environment
cd deploy/terraform/live/dev/event-platform
terragrunt plan
terragrunt apply

# Apply every component in an environment at once
cd deploy/terraform/live/dev
terragrunt run-all apply

# Promote a specific image build to prod
cd deploy/terraform/live/prod/event-platform
terragrunt apply -var 'image_tag=1.2.3'
```

Override the cluster/kubeconfig without editing files:

```bash
KUBECONFIG=/path/to/kubeconfig terragrunt apply
```

## Validating the modules without a cluster

```bash
cd deploy/terraform/modules/event-platform
terraform init -backend=false
terraform validate
```

## Design notes

- **Replica counts are autoscaler-owned.** Deployments set only a floor and
  `ignore_changes = [spec[0].replicas]`, so Terraform never fights HPA/KEDA.
- **KEDA via `kubectl_manifest`.** The `ScaledObject` is applied with the
  `gavinbunney/kubectl` provider to avoid the `kubernetes_manifest` plan-time
  CRD-availability limitation.
- **DRY.** Common module wiring lives in `_envcommon/event-platform.hcl`; each
  env file only overrides what differs (endpoints, scaling, image tag).

