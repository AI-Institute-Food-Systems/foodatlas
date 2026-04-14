# FoodAtlas Infrastructure

This directory holds everything needed to run FoodAtlas in the two operating modes the project supports:

- **Local development** — A single-machine Docker Compose stack with PostgreSQL, used while writing code and running tests on a developer laptop.
- **AWS production** — A multi-stack AWS CDK deployment (RDS, ECS Fargate, ALB, S3, ECR, Secrets Manager) that hosts the live API for the Vercel frontend.

The two modes share **zero physical infrastructure**. Local Postgres lives in a Docker container on your laptop; production Postgres is RDS in `us-west-1`. The only thing in common is the Python code in `backend/` and the Alembic schema in `backend/db/migrations/`. Everything else — credentials, networking, deploy steps — diverges by mode.

---

## Contents

```
infra/
├── README.md              # This file
├── local/                 # Mode: local development
│   ├── README.md          # Local quick-start
│   ├── docker-compose.yml # Local Postgres 16
│   ├── init-db.sql        # Postgres init script (enables pg_trgm)
│   └── scripts/
│       └── run_monthly.sh # Local IE → KGC → DB → S3 orchestrator
└── cdk/                   # Mode: AWS production
    ├── README.md          # CDK CLI quick reference
    ├── app.py             # CDK app entry point
    ├── stacks/            # Six stack definitions
    ├── tests/             # Snapshot tests using aws_cdk.assertions
    └── scripts/           # ECS one-off task runners (migrations, ETL)
```

---

## Mode 1: Local development

The local stack is the path of least resistance for iterating on code. Everything runs on your laptop, no AWS account required.

### Architecture

Arrows show request flow at runtime. Everything runs as a process or container on the developer laptop.

```mermaid
flowchart LR
    subgraph laptop["Developer laptop"]
        next["Next.js<br/>npm run dev<br/>:3001"]
        api["FastAPI<br/>uv run python main.py<br/>:8000"]
        pg[("PostgreSQL 16<br/>Docker container<br/>:5432")]
    end

    next -->|"NEXT_PUBLIC_API_URL=<br/>http://localhost:8000"| api
    api -->|"DB_HOST=localhost"| pg
```

### Bring it up

```
docker compose -f infra/local/docker-compose.yml up -d
cd backend/db && uv run alembic upgrade head
cd backend/db && uv run python main.py load
cd backend/api && uv run python main.py
cd frontend && npm run dev
```

The Docker Postgres uses `foodatlas` / `foodatlas` / `foodatlas` for db / user / password — these are committed into `docker-compose.yml` deliberately. The container binds to localhost and is not reachable from the network, so the weak credentials are fine for dev.

### `run_monthly.sh`

A long-running orchestrator that chains the IE → KGC → DB → S3 stages. Used to refresh the knowledge graph from biomedical literature each month. Reads source `.env` files for API keys (`OPENAI_API_KEY`, `NCBI_API_KEY`, `NCBI_EMAIL`) and shells out to the corresponding sub-projects. The S3 upload step calls `backend/kgc/scripts/sync-outputs-to-s3.sh`, which auto-resolves the bucket from `FoodAtlasStorageStack`.

```
bash infra/local/scripts/run_monthly.sh                     # Full run
bash infra/local/scripts/run_monthly.sh --skip-ie           # Reuse existing IE outputs
bash infra/local/scripts/run_monthly.sh --ie-only           # IE only, stop before KGC
```

---

## Mode 2: AWS production

Production runs on AWS and is defined entirely in CDK Python under `infra/cdk/`. The infrastructure is split into six stacks; each is independently deployable but they have constructor dependencies that CDK uses to enforce deploy order.

### Architecture

```mermaid
flowchart TB
    vercel["Vercel<br/>(Next.js)"]
    alb["Application<br/>Load Balancer<br/>(public subnet)"]
    api["ECS Fargate<br/>FastAPI task<br/>(public subnet)"]
    rds[("RDS PostgreSQL 16<br/>db.t4g.small<br/>Single-AZ<br/>(isolated subnet)")]
    secrets[("Secrets Manager<br/>(DB credentials)")]
    s3[("S3 bucket<br/>data/&lt;ts&gt;/<br/>outputs/&lt;ts&gt;/")]
    jobs["ECS Fargate<br/>one-off task<br/>(migrations + ETL)"]
    ecr_api[("ECR<br/>foodatlas-api")]
    ecr_db[("ECR<br/>foodatlas-db")]

    vercel -->|"HTTPS<br/>(HTTP today)"| alb
    alb -->|":8000"| api
    api -->|":5432"| rds
    api -->|"fetch at start"| secrets
    api -->|"image pull"| ecr_api
    jobs -->|":5432"| rds
    jobs -->|"fetch at start"| secrets
    jobs -->|"image pull"| ecr_db
    jobs -->|"download"| s3
```

### The six stacks

| Stack | Responsibility | Resources |
|---|---|---|
| `FoodAtlasNetworkStack` | VPC, subnets, security groups | 1 VPC, 2 AZs, public + isolated subnets, no NAT |
| `FoodAtlasStorageStack` | S3 bucket for KGC artifacts | 1 versioned encrypted bucket with lifecycle rules |
| `FoodAtlasEcrStack` | Container registries | 2 ECR repos (`foodatlas-api`, `foodatlas-db`), each with `keep-last-10` lifecycle |
| `FoodAtlasDatabaseStack` | Postgres + credentials | RDS db.t4g.small Single-AZ, Secrets Manager secret |
| `FoodAtlasApiStack` | Long-running API service | ECS cluster, task def, service, ALB, log group |
| `FoodAtlasJobsStack` | One-off task runner | Task def + log group + SG (no service) |

The Single-AZ RDS choice means an AZ-level outage requires ~15 minutes to recover from automated backup. Flip `multi_az=True` in `database_stack.py` to upgrade to true high availability.

### Stack dependencies

```mermaid
flowchart TB
    network[NetworkStack]
    storage[StorageStack]
    ecr[EcrStack]
    db[DatabaseStack]
    api[ApiStack]
    jobs[JobsStack]

    network --> db
    network --> api
    network --> jobs
    storage --> api
    storage --> jobs
    ecr --> api
    ecr --> jobs
    db --> api
    db --> jobs
    api -.->|"ApiCluster"| jobs
```

`JobsStack` reuses the ECS cluster from `ApiStack` — cluster reuse is free and reduces operational surface.

---

## First-time deploy

This is what a fresh AWS account looks like. Skip steps you've already done.

### Prerequisites

- AWS account with administrator-equivalent access (via SSO or IAM user)
- AWS CLI configured: `aws configure sso` (or `aws configure` for static keys)
- Docker daemon running locally (used by image build steps)
- `npm install -g aws-cdk`
- `uv` installed
- `cd infra/cdk && uv sync`

### CDK bootstrap (once per account+region)

```
cdk bootstrap aws://YOUR_ACCOUNT/us-west-1
```

This creates the `CDKToolkit` stack which provisions an S3 bucket and an ECR repo that CDK uses for asset publishing.

### Deploy the foundation stacks

```
cd infra/cdk
uv run cdk deploy FoodAtlasNetworkStack FoodAtlasStorageStack FoodAtlasEcrStack
```

`FoodAtlasNetworkStack` and `FoodAtlasStorageStack` deploy in parallel (no dependency between them). `FoodAtlasEcrStack` deploys after to create the ECR repos.

### Build and push the container images

The API and one-off jobs need their Docker images in ECR before the consuming stacks can start tasks. Both push scripts hardcode the account/region/repo URI.

```
./backend/api/scripts/push-to-ecr.sh         # Builds and pushes foodatlas-api:latest
./backend/db/scripts/push-to-ecr.sh          # Builds and pushes foodatlas-db:latest
```

Both scripts pass `--platform linux/amd64` because the Fargate task definitions pin `ecs.CpuArchitecture.X86_64`. They accept an optional positional tag (`./push-to-ecr.sh abc1234`) to push a specific version; default is `latest`.

### Deploy the database

```
uv run cdk deploy FoodAtlasDatabaseStack
```

This provisions RDS, which takes 10–15 minutes. **Billing starts here** — RDS is the largest line item in the stack. The auto-generated master password is stored in AWS Secrets Manager (path defined by `_DB_SECRET_NAME` in `cdk/stacks/database_stack.py`); nothing on your laptop ever sees the plaintext.

### Deploy the API and jobs stacks

```
uv run cdk deploy FoodAtlasApiStack FoodAtlasJobsStack
```

`ApiStack` brings up the ALB + ECS service + 1 Fargate task pulling `foodatlas-api:latest` from ECR. `JobsStack` registers the one-off task definition; no tasks run until you invoke them via the helper scripts.

### Run schema migrations

```
cd infra/cdk
./scripts/run-migration.sh
```

Invokes a one-off Fargate task that runs `alembic upgrade head` against RDS. The task uses the `foodatlas-db` image, fetches credentials from Secrets Manager, exits in ~30 seconds. The script tails CloudWatch logs while it runs.

### Publish KGC content and load it into RDS

```
./backend/kgc/scripts/sync-data-to-s3.sh        # One-time per data refresh
./backend/kgc/scripts/sync-outputs-to-s3.sh     # Once per KGC run
cd infra/cdk && ./scripts/run-data-load.sh
```

After this completes, the API is serving real data. Test with:

```
curl http://<ALB-dns>/health                    # Returns {"status":"ok"}
curl http://<ALB-dns>/food/metadata?common_name=apple
```

Get the ALB DNS from `aws cloudformation describe-stacks --stack-name FoodAtlasApiStack --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text`.

---

## S3 layout

`FoodAtlasStorageStack` creates one bucket with two top-level prefixes. Each prefix carries its own version history and `LATEST` pointer, because source data and pipeline outputs evolve at different cadences.

```
s3://<kgcbucket>/
├── data/                                ← source ontologies, refreshed quarterly
│   ├── 20260413T221503Z/
│   │   ├── CDNO/, ChEBI/, CTD/, FDC/, FlavorDB/,
│   │   ├── FoodOn/, HSDB/, MeSH/, PubChem/    (anything in backend/kgc/data/
│   │   └── ...                                 except PreviousFAKG/ and Lit2KG/)
│   └── LATEST                           ← text file with the timestamp string
└── outputs/                             ← KGC pipeline output, refreshed monthly
    ├── 20260413T221503Z/
    │   ├── manifest.json                ← data_version + git_sha + host + user
    │   ├── kg/
    │   │   ├── entities.parquet
    │   │   ├── entity_registry.parquet
    │   │   ├── triplets.parquet
    │   │   ├── relationships.parquet
    │   │   ├── evidence.parquet
    │   │   ├── attestations.parquet
    │   │   ├── attestations_ambiguous.parquet
    │   │   ├── retired.parquet
    │   │   ├── checkpoints/             ← debugging sidecars
    │   │   ├── diagnostics/
    │   │   └── intermediate/
    │   └── ingest/
    │       └── (per-source ingestion outputs)
    └── LATEST
```

**Why split data and outputs.** Source ontologies are 10+ GB and refresh quarterly. KGC outputs are smaller (~hundreds of MB) and refresh monthly. If we bundled them, every monthly KGC run would re-upload the entire source tree.

**LATEST pointers.** Single-line text files containing the timestamp of the most recent run for that prefix. Downstream tooling reads these to resolve "current" without listing the bucket. The `outputs/<ts>/manifest.json` records which `data/<ts>/` was current at the time of the run, so every output version has a traceable upstream data version.

**Versioning is forever.** No lifecycle rule deletes old `data/` or `outputs/` versions — parquet is small enough that long-term retention is not a concern. The bucket also has S3 object versioning enabled as an additional safety net against accidental deletes.

**Excluded from data sync:** `PreviousFAKG/` (downloaded back from S3 — would create a loop), `Lit2KG/` (legacy text-parser outputs no longer used by the pipeline), plus repo housekeeping (`README.md`, `.gitignore`, `download.sh`).

---

## Helper scripts

Two categories: **artifact publishing** (lives next to the artifact being published) and **cloud orchestration** (lives in `infra/cdk/scripts/`).

### Artifact publishing

| Script | Action |
|---|---|
| `backend/api/scripts/push-to-ecr.sh [tag]` | Build the API Docker image, tag with `[tag]` (default `latest`), push to `foodatlas-api` ECR repo |
| `backend/db/scripts/push-to-ecr.sh [tag]` | Same for the db jobs image (`foodatlas-db`) |
| `backend/kgc/scripts/sync-data-to-s3.sh` | Upload `backend/kgc/data/` to `s3://<bucket>/data/<ts>/`, bump `data/LATEST`. Excludes `PreviousFAKG/`, `Lit2KG/`, repo housekeeping |
| `backend/kgc/scripts/sync-outputs-to-s3.sh` | Upload `backend/kgc/outputs/` to `s3://<bucket>/outputs/<ts>/`, write `manifest.json`, bump `outputs/LATEST` |
| `backend/kgc/scripts/pull-data-from-s3.sh [version]` | Download `data/<latest>/` (or explicit `[version]`) into `backend/kgc/data/` |
| `backend/kgc/scripts/pull-from-s3.sh [version]` | Download `outputs/<latest>/kg/` into `backend/kgc/data/PreviousFAKG/<ts>/` (the baseline for the next KGC run) |

### Cloud orchestration

| Script | Action |
|---|---|
| `infra/cdk/scripts/run-migration.sh [revision]` | Invoke `alembic upgrade <revision>` on RDS via a one-off Fargate task (default `head`). Tails logs |
| `infra/cdk/scripts/run-data-load.sh [version]` | Invoke the ETL loader against `s3://<bucket>/outputs/<latest>/kg/` (or `[version]`). Tails logs |
| `infra/cdk/scripts/_lib.sh` | Shared bash helpers: read CFN outputs, build network config, invoke + wait + tail |

---

## Common workflows

### Ship an API code change

```
# Local: write code + tests
cd backend/api && uv run pytest

# Build + push the new image
./backend/api/scripts/push-to-ecr.sh $(git rev-parse --short HEAD)

# Deploy the API stack pinning the new tag (immutable)
cd infra/cdk
uv run cdk deploy FoodAtlasApiStack -c api_image_tag=$(git rev-parse --short HEAD)
```

ECS performs a rolling deploy. The default `min_healthy_percent=50%` with `desired_count=1` means there's a brief window (~30s) where the API is unavailable during rollover. Acceptable at current scale; bump `desired_count=2` for zero-downtime.

### Add a database column

```
# Update SQLAlchemy model
vim backend/db/src/models/entities.py

# Generate migration (don't edit 001_initial_schema.py)
cd backend/db
uv run alembic revision --autogenerate -m "add my_column to entities"
# Review the generated 002_*.py file, edit if autogen missed anything

# Test against local Postgres
uv run alembic upgrade head

# Commit, deploy through CD (or manually):
./backend/db/scripts/push-to-ecr.sh $(git rev-parse --short HEAD)
cd infra/cdk && uv run cdk deploy FoodAtlasJobsStack -c db_image_tag=$(git rev-parse --short HEAD)
./scripts/run-migration.sh
# Then deploy the API code that depends on the new column
```

The migration must run **before** the API code that depends on the new column. Reverse order causes 500s.

### Roll back the data to a previous KGC run

```
# List available versions
aws s3 ls s3://<bucket>/outputs/

# Load a specific timestamp
cd infra/cdk
./scripts/run-data-load.sh 20260315T140000Z
```

The loader is destructive (rewrites tables), so this fully replaces the current data. The previous version's parquet stays in S3 forever.

### Roll back the API to a previous image

```
cd infra/cdk
uv run cdk deploy FoodAtlasApiStack -c api_image_tag=<previous-sha>
```

ECS rolling deploy, ~5 minutes. Image lifecycle keeps the last 10 in ECR.

---

## Troubleshooting

### `Cannot delete export ... in use`

You renamed a CDK resource that's referenced by another stack. CloudFormation refuses to remove the cross-stack export until consumers stop importing it. Two-phase fix:

1. Keep the old resource alongside the new one in the producer stack. Use `self.export_value(old_resource.attr, name="<exact-old-export-name>")` to pin the old export.
2. Deploy the producer first (creates the new resource and preserves the old export), then redeploy consumers (they switch to the new export).
3. Once nothing imports the old export, remove the old resource and pinned exports from the producer in a follow-up deploy.

### ECR push fails with `name unknown`

The ECR repo doesn't exist yet. `cdk deploy FoodAtlasEcrStack` first.

### ECS task fails to pull image with `manifest unknown`

You're trying to deploy `ApiStack` or `JobsStack` before pushing the corresponding image. Push first.

### Stack deploy hangs at "service waiting to stabilize"

ECS service is waiting for the task to pass health checks. The container might be crashing on startup. Check `aws logs tail /aws/ecs/<log-group>` for the actual error. Common causes: wrong DB host, bad secret name, container can't reach RDS due to security group misconfiguration.

### `aws sso login` says token expired

```
aws sso login
```

SSO sessions last 12 hours. CDK and AWS CLI both fail with cryptic auth errors when expired.

### Mixed content errors in browser when calling API from Vercel

ALB is HTTP-only. Browsers block HTTPS pages from making HTTP requests. You need ACM + Route 53 + a 443 listener on the ALB. Tracked as a deferred plan item.

### Orphan bucket left after a rename

`RemovalPolicy.RETAIN` keeps the physical bucket when the CDK resource is deleted. Manually clean up:

```
aws s3 rb s3://<orphan-bucket-name>          # If empty
aws s3 rb s3://<orphan-bucket-name> --force  # If you also want to delete contents (DESTRUCTIVE)
```

### Stack drift (something CDK manages was changed manually)

```
aws cloudformation detect-stack-drift --stack-name FoodAtlasFooStack
aws cloudformation describe-stack-resource-drifts --stack-name FoodAtlasFooStack
```

Fix by either reverting the manual change in the AWS console, or `cdk destroy` + `cdk deploy` to recreate from CDK state. Don't edit AWS resources by hand if you can help it — drift makes deploys unpredictable.

---

## What's deferred

- **HTTPS / custom domain.** ALB currently listens on port 80 only. ACM cert + Route 53 + 443 listener is a small CDK addition once a domain is wired up.
- **CD pipelines.** Today everything is `cdk deploy` from a developer laptop. A GitHub Actions workflow with OIDC role assumption can build images, push to ECR, and deploy on merge. App-code CD is the highest-ROI first step; infra CD should keep a manual approval gate.
- **Multi-AZ RDS.** Currently Single-AZ. One flag flip (`multi_az=True` in `database_stack.py`) to upgrade when the project warrants it.
- **AWS-hosted KGC pipeline.** KGC currently runs on a developer machine and uploads outputs via the sync scripts. Future option: AWS Batch or Step Functions for fully managed runs.

---

## Reference

- CDK Python API docs: https://docs.aws.amazon.com/cdk/api/v2/python/
- CDK construct library: https://constructs.dev/
- AWS CLI command reference: https://docs.aws.amazon.com/cli/latest/reference/
- For CDK CLI commands (synth, diff, deploy, destroy): see [`cdk/README.md`](cdk/README.md)
