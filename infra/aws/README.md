# FoodAtlas AWS

AWS CDK Python project that defines the FoodAtlas production infrastructure. **For architecture, stack inventory, deploy phases, scripts, troubleshooting, and the local-vs-AWS overview, see [`../README.md`](../README.md).** This file is a CDK CLI cheat sheet.

## Setup

```
cd infra/aws
uv sync
```

Requires:
- Python 3.12+
- `uv`
- Node.js 20+ (the `cdk` CLI is a Node binary)
- `npm install -g aws-cdk`
- AWS credentials configured (`aws configure sso` or static keys)

## Stacks

| Logical ID | Module | Depends on |
|---|---|---|
| `FoodAtlasNetworkStack` | `stacks/network_stack.py` | — |
| `FoodAtlasStorageStack` | `stacks/storage_stack.py` | — |
| `FoodAtlasDownloadsStack` | `stacks/downloads_stack.py` | — |
| `FoodAtlasEcrStack` | `stacks/ecr_stack.py` | — |
| `FoodAtlasDatabaseStack` | `stacks/database_stack.py` | Network |
| `FoodAtlasApiStack` | `stacks/api_stack.py` | Network, Storage, Downloads, Ecr, Database |
| `FoodAtlasJobsStack` | `stacks/jobs_stack.py` | Network, Storage, Ecr, Database, Api |

CDK resolves deploy order from constructor dependencies in `app.py`. You can `cdk deploy <stack>` and dependents are pulled in automatically.

## Common CDK commands

```
uv run cdk synth                          # Synthesize all stacks to cdk.out/
uv run cdk synth FoodAtlasApiStack        # Print one stack's CloudFormation template
uv run cdk diff                           # Diff all stacks against deployed state
uv run cdk diff FoodAtlasStorageStack     # Diff one stack — always do this before deploy
uv run cdk deploy FoodAtlasApiStack       # Deploy one stack (and its dependencies)
uv run cdk deploy --all                   # Deploy every stack in the app
uv run cdk destroy FoodAtlasJobsStack     # Tear down a stack
uv run cdk ls                             # List all stack IDs in the app
```

## Context variables

```
uv run cdk deploy FoodAtlasApiStack -c api_image_tag=abc1234
uv run cdk deploy FoodAtlasJobsStack -c db_image_tag=abc1234
```

`api_image_tag` and `db_image_tag` default to `latest`. Use immutable git SHAs in production deploys for predictable rollbacks.

## Tests

```
uv run pytest
```

Tests under `tests/` use `aws_cdk.assertions.Template` to assert the synthesized CloudFormation has the expected resources and properties. They run in CI (`infra-aws-test` job) on every PR touching `infra/aws/`.

## Ad-hoc scripts

The `scripts/` directory holds runners for one-off ECS tasks (ETL data load). They're documented in [`../README.md#helper-scripts`](../README.md#helper-scripts) and read CFN outputs from `FoodAtlasJobsStack` to invoke `aws ecs run-task` correctly.

## File layout

```
aws/
├── app.py                  # CDK app entry point
├── cdk.json                # CDK CLI config (app command, feature flags)
├── pyproject.toml          # Dependencies + pytest config
├── stacks/                 # One module per stack (network, storage, downloads,
│                           # ecr, database, api, jobs)
├── tests/                  # One snapshot-test module per stack
└── scripts/
    ├── _lib.sh             # Shared helpers
    └── run-data-load.sh
```
