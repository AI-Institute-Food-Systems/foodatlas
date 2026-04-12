# FoodAtlas CDK

AWS CDK infrastructure for FoodAtlas. Deploys RDS PostgreSQL, ECS Fargate
(FastAPI), an Application Load Balancer, and an S3 bucket for KGC parquet
outputs.

## Prerequisites

- AWS account with credentials configured (`aws configure` or env vars)
- CDK bootstrapped in the target account/region: `cdk bootstrap aws://ACCOUNT/REGION`
- Node.js 20+ (for the `cdk` CLI): `npm install -g aws-cdk`
- Python 3.12+, `uv`

## Setup

```bash
cd infra/cdk
uv sync
```

## Deploy

```bash
# Deploy foundation stacks first (order is enforced by dependencies)
uv run cdk deploy FoodAtlasNetworkStack FoodAtlasStorageStack FoodAtlasDatabaseStack

# Build & push the API image to ECR, then deploy ApiStack
# (see repo root docs for the push command)
uv run cdk deploy FoodAtlasApiStack
```

## Test

```bash
uv run pytest
```

Snapshot tests under `tests/` use `aws_cdk.assertions.Template` to verify
expected resources and properties.

## Synth

```bash
uv run cdk synth
```

Writes CloudFormation templates to `cdk.out/`.
