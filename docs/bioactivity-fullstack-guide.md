# FoodAtlas Bioactivity — Full-Stack Developer Guide

**Status:** ✅ Staging preview is **live** and serving real data.
**Audience:** the full-stack developer building the bioactivity feature (frontend **and** backend).
**Last updated:** 2026-06-22

This guide is everything you need to (1) run the whole stack locally, (2) build the
bioactivity UI, (3) change the backend (API / database / data) and redeploy the staging
environment yourself, and (4) ship it to the live site.

---

## Table of contents

1. [Overview & scope](#1-overview--scope)
2. [System architecture](#2-system-architecture)
3. [Environments & instances](#3-environments--instances)
4. [Repository layout](#4-repository-layout)
5. [Local development (full stack)](#5-local-development-full-stack)
6. [The bioactivity API](#6-the-bioactivity-api)
7. [Where response formats are defined](#7-where-response-formats-are-defined)
8. [Changing the backend & redeploying staging](#8-changing-the-backend--redeploying-staging)
9. [Going live (production)](#9-going-live-production)
10. [Phase 2 (later): bioactivity–disease](#10-phase-2-later-bioactivitydisease)
11. [Reference & troubleshooting](#11-reference--troubleshooting)

---

## 1. Overview & scope

FoodAtlas is adding a new data domain: **bioactivity** (e.g. *antioxidant*, *anticancer*,
*anti-inflammatory*). Three relationship kinds, plus measurement detail:

- **Bioactivity concepts** — 21 of them, in a small hierarchy (e.g. `antibacterial → antimicrobial → antiinfective`).
- **Foods *exhibit* bioactivities** — e.g. *onion exhibits antioxidant activity* (~1,934 links).
- **Chemicals are *measured* for bioactivities** — e.g. *quercetin measured for anticancer*, backed by lab assays (~177,326 links).
- **Measurements** — each chemical↔bioactivity link is backed by individual assay measurements: outcome (Active/Inactive/…), endpoint (IC50/AC50/EC50/Ki…), potency value + unit (~1.8M rows total).

> **Scope of this release:** *food ↔ bioactivity* and *chemical ↔ bioactivity* only.
> **Disease** associations are a separate later phase (§10) — no disease data, API, or UI here.

---

## 2. System architecture

FoodAtlas is a polyglot monorepo. Data flows left-to-right through four stages; you'll
touch all of them:

```
  backend/kgc            backend/db                  backend/api          frontend
  ───────────            ──────────                  ───────────          ────────
  KGC pipeline   ──→     ETL loader        ──→       FastAPI       ──→    Next.js 14
  (knowledge     parquet (drops+recreates   reads    (port 8000)   HTTP   (port 3001)
   graph)        files   schema, loads,     mv_*      Bearer key          NEXT_PUBLIC_*
                         builds mv_* views)  views
                              │
                              ▼
                         PostgreSQL 16
```

- **`backend/kgc`** — builds the knowledge graph and emits **parquet** files
  (`entities.parquet`, `triplets.parquet`, `attestations_bioactivity.parquet`, …).
- **`backend/db`** — the ETL **loader** reads those parquet files, **drops & recreates**
  the schema, loads base tables, then builds the **materialized views** (`mv_*`) the API serves.
- **`backend/api`** — FastAPI. Routes → repositories → `mv_*` views. **Never reads base tables.**
- **`frontend`** — Next.js 14 (App Router). Calls the API over HTTP with a Bearer key.

**Key principle — the API only ever reads `mv_*` views.** To change what an endpoint
returns you change (a) the SQL in the repository, and/or (b) the view's columns, and/or
(c) how the ETL builds the view. See §7.

**Bioactivity in each layer:**

| Layer | Bioactivity artifact |
|---|---|
| KGC | `attestations_bioactivity.parquet` + bioactivity entities/triplets in `entities`/`triplets` |
| DB base table | `base_attestations_bioactivity` (the raw measurements) |
| DB views | `mv_bioactivity_entities`, `mv_chemical_bioactivity`, `mv_food_bioactivity` |
| API | `/bioactivity/*`, `/chemical/bioactivities`, `/food/bioactivities` |

---

## 3. Environments & instances

Two complete, isolated environments live in the **same AWS account** (`030635937737`,
region `us-west-1`). Production is **untouched** by bioactivity until launch.

| | **Production (live)** | **Staging (bioactivity preview)** |
|---|---|---|
| Purpose | the public site | your dev/preview backend |
| API URL | live ALB | *fetch via §3 → Getting access* (`ApiUrl` stack output) |
| RDS instance | `…databasestack-postgresinstance…-sgltpjtapjkg` | `…databasestack-st-postgresinstance…-qj5oikr04ga8` |
| ECS cluster | `…ApiCluster…-WqnGaFsCeFCE` | `FoodAtlasApiStack-Staging-ApiCluster7CE9CBE6-BtVAz1IpCkmx` |
| ECS service | `…ApiService…` (prod) | `FoodAtlasApiStack-Staging-ApiService199661B5-QAbwy3lSlebq` |
| DB secret | `foodatlas/db/credentials` | `foodatlas/db/credentials-staging` |
| Internal API key | (prod `ApiKeySecret`) | *fetch via §3 → Getting access* (Secrets Manager) |
| CloudFormation | `FoodAtlas{Database,Api,Jobs}Stack` | `FoodAtlas{Database,Api,Jobs}Stack-Staging` |
| Docker image tag | pinned SHA (e.g. `0a019f908ca0`) | **`bioactivity`** |
| S3 KG data | `s3://…kgcbucket…/outputs/LATEST → <version>` | `s3://…kgcbucket…/outputs/staging-bioactivity/kg/` |

**Shared by both** (do not duplicate): the VPC (`FoodAtlasNetworkStack`), S3 buckets
(`FoodAtlasStorageStack`, `FoodAtlasDownloadsStack`), and ECR repos (`foodatlas-api`,
`foodatlas-db`). Full IDs are in §11.

### Getting access & credentials — start here

This guide is **safe to commit: it contains no secrets.** Get the two values your frontend
needs — `NEXT_PUBLIC_API_URL` and `NEXT_PUBLIC_API_KEY` — in one of two ways.

**Option 1 — ask the maintainer (no AWS account needed).** If you're only building the
frontend, ask the FoodAtlas maintainer for the staging **API URL** and **API key**, put them
in `frontend/.env.local` (§5), and you're set for Mode A.

**Option 2 — fetch them yourself (needs AWS access).**

*a) Get AWS access.* Ask the maintainer to add you to the FoodAtlas AWS account
(`030635937737`, region `us-west-1`) and to give you the SSO **start URL** + a profile name. Then, one-time:
```bash
aws configure sso                        # paste the SSO start URL; region = us-west-1
aws sso login --profile <your-profile>   # re-run whenever the token expires
export AWS_PROFILE=<your-profile> AWS_REGION=us-west-1
```

*b) Fetch `NEXT_PUBLIC_API_URL`* (the staging ALB):
```bash
aws cloudformation describe-stacks --stack-name FoodAtlasApiStack-Staging \
  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text
```

*c) Fetch `NEXT_PUBLIC_API_KEY`* (the staging internal key):
```bash
ARN=$(aws cloudformation describe-stacks --stack-name FoodAtlasApiStack-Staging \
  --query "Stacks[0].Outputs[?OutputKey=='ApiKeySecretArn'].OutputValue" --output text)
aws secretsmanager get-secret-value --secret-id "$ARN" --query SecretString --output text
```

Keep both **out of git** — they belong only in `frontend/.env.local` (gitignored) or a local
`staging.env`. The key is staging-only and rotatable.

**Cost.** Staging runs ~$80/mo while up (RDS + 2 Fargate tasks + ALB). Tear it down any
time with `cdk destroy 'FoodAtlas*-Staging'`; redeploy with §8.

---

## 4. Repository layout

```
foodatlas/
├── frontend/                     Next.js 14 app (port 3001)
│   ├── utils/fetching.ts         ← API client (Bearer key); add bioactivity fetchers here
│   ├── middleware.ts             reads NEXT_PUBLIC_API_URL / NEXT_PUBLIC_API_KEY
│   └── next.config.mjs           rewrites/proxy config
├── backend/
│   ├── api/                      FastAPI (port 8000)
│   │   └── src/
│   │       ├── routes/           bioactivity.py, chemical.py, food.py  (endpoints)
│   │       ├── repositories/     bioactivity.py  (SQL → JSON; the response shape)
│   │       ├── dependencies.py   DBSettings (DB_* env), get_db, verify_api_key
│   │       └── config.py         APISettings (API_* env)
│   ├── db/                       ETL + schema (drop-and-recreate each load)
│   │   ├── main.py               `load` CLI command
│   │   └── src/
│   │       ├── models/views.py   mv_* view definitions (columns)
│   │       ├── models/attestations_bioactivity.py   base_attestations_bioactivity
│   │       └── etl/materializer_bioactivity.py      builds the bioactivity mv_*
│   └── kgc/                      knowledge-graph pipeline (emits parquet)
│       └── outputs/kg/           parquet output the DB loads
├── infra/
│   ├── aws/                      CDK (stacks/*.py, app.py); scripts/run-data-load.sh
│   └── local/                    docker-compose.yml (local Postgres 16)
└── docs/                         ← you are here (this guide + the API tester + examples)
```

---

## 5. Local development (full stack)

You have two modes. Pick based on what you're changing.

### Mode A — frontend only (point at staging)
No local backend needed. In your frontend repo:
```bash
# frontend/.env.local
NEXT_PUBLIC_API_URL=<staging API URL>     # fetch via §3 → Getting access, or ask the maintainer
NEXT_PUBLIC_API_KEY=<staging API key>     # fetch via §3 → Getting access, or ask the maintainer
```
```bash
cd frontend && npm install && npm run dev      # http://localhost:3001
```
> The staging API allows CORS from `http://localhost:3000` and `http://localhost:3001`.
> Server-side calls (RSC / route handlers) aren't subject to CORS at all.

### Mode B — full stack local (when changing backend/data)
Prerequisites: `docker`, `uv` (Python 3.12), `node`, `aws` CLI.

```bash
# 1) Postgres 16 (creds foodatlas/foodatlas/foodatlas, db "foodatlas", 127.0.0.1:5432)
docker compose -f infra/local/docker-compose.yml up -d

# 2) Load the KG into Postgres (drops & recreates schema, loads, builds mv_*)
cd backend/db && uv run python main.py load
#    └ default --parquet-dir = backend/kgc/outputs/kg
#    └ or load straight from the staging S3 copy:
#      uv run python main.py load --parquet-dir s3://<kgc-bucket>/outputs/staging-bioactivity/kg/

# 3) API (port 8000). API_DEBUG defaults True locally → no API key required.
cd backend/api && uv run python main.py

# 4) Frontend (point at local API)
#    frontend/.env.local:
#      NEXT_PUBLIC_API_URL=http://localhost:8000
#      NEXT_PUBLIC_API_KEY=anything            # ignored when API_DEBUG=True
cd frontend && npm run dev
```

**Environment variables that matter** (all read from each sub-project's `.env` or the shell):

| Var | Where | Local default | Notes |
|---|---|---|---|
| `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD` | api + db (`DB_` prefix) | `localhost:5432` / `foodatlas`×3 | API & loader DB connection |
| `API_DEBUG` | api (`API_` prefix) | `True` | **`True` skips the API-key check** + rate limit |
| `API_KEY` | api | empty | the internal Bearer key (set on staging/prod) |
| `API_CORS_ORIGINS` | api | `http://localhost:3000` | comma-separated allowed origins |
| `NEXT_PUBLIC_API_URL` / `NEXT_PUBLIC_API_KEY` | frontend | — | the API base + Bearer key |

Verify locally:
```bash
curl -s "http://localhost:8000/bioactivity/metadata?common_name=antioxidant" | python3 -m json.tool
```

---

## 6. The bioactivity API

**Conventions:** `GET` with `?common_name=<term>`, behind a Bearer key
(`Authorization: Bearer <key>`). Every response is wrapped:

```json
{ "data": [ /* records */ ], "metadata": { "row_count": 0 } }
```

Records use **`name`** and **`id`** (the FoodAtlas id). `potency_summary` is a **list** of
`{ "endpoint", "unit", "median", "n" }`.

### Try it / regenerate examples
[`./query-bioactivity-api.sh`](query-bioactivity-api.sh) calls all five endpoints and saves
real responses into [`bioactivity-api-examples/`](bioactivity-api-examples/). **Those files
are the authoritative shapes.** First give it the URL + key (§3 → *Getting access*), then run:
```bash
cd docs
cp staging.env.example staging.env        # then fill in API_URL + API_KEY (keep gitignored)
./query-bioactivity-api.sh                # uses staging.env
# …or pass them inline (e.g. against a local backend) + change the query:
API_URL=http://localhost:8000 API_KEY=dev ./query-bioactivity-api.sh --chemical resveratrol --food garlic
```

### Endpoint reference

| Endpoint | Returns | Example query → rows |
|---|---|---|
| `GET /bioactivity/metadata` | one concept: hierarchy + counts | antioxidant → 1 |
| `GET /bioactivity/foods` | foods that exhibit it | antioxidant → 972 |
| `GET /bioactivity/chemicals` | chemicals measured for it | antioxidant → 11,109 |
| `GET /chemical/bioactivities` | a chemical's bioactivities + measurements | quercetin → 17 |
| `GET /food/bioactivities` | a food's bioactivities | onion → 2 |

**Concept** (`/bioactivity/metadata?common_name=antioxidant`):
```json
{ "data": [{
  "common_name": "antioxidant", "id": "e227382",
  "synonyms": ["antioxidant", "anti-oxidant", "…"],
  "description": "Bioactivity refers to the ability of substances to inhibit … oxidation …",
  "external_ids": { "bioactivity_concept": ["E300002"] },
  "parents": [], "children": [],
  "n_foods": 972, "n_chemicals": 11109
}], "metadata": { "row_count": 1 } }
```

**Chemical → bioactivities** (`/chemical/bioactivities?common_name=quercetin`):
```json
{ "data": [{
  "name": "anticancer", "id": "e227383",
  "measurement_count": 755, "active_count": 83, "inactive_count": 261,
  "potency_summary": [{ "endpoint": "IC50", "unit": "MICROMOLAR", "median": 17.175, "n": 130 }],
  "measurements": [{ "assay": "AID: 364", "outcome": "Active", "endpoint": "AC50",
                     "value": 0.035, "unit": "MICROMOLAR" }]
}], "metadata": { "row_count": 17 } }
```
> `measurements` is a capped representative sample for display. `value` is `null` for
> non-potency outcomes (e.g. Inactive). Units are **not** normalized — expect mixed
> `MICROMOLAR` / `nM` / `""`; treat `(endpoint, unit)` as the grouping key.

**Food → bioactivities** (`/food/bioactivities?common_name=onion`) and
**concept → foods/chemicals** follow the same envelope; see the example JSON files for full shapes.

### Auth & CORS (summary)
- Bearer key required on staging/prod (`API_DEBUG=False`); skipped locally (`API_DEBUG=True`).
- Staging CORS: `localhost:3000`, `localhost:3001`, the `foodatlas.ai` origins. Need another origin? See §8.4.
- Endpoints are unlisted (`include_in_schema=False`) but fully callable.

---

## 7. Where response formats are defined

There are **no Pydantic response models** — routes return the repository dict and FastAPI
serializes it. So the SQL + the view + the ETL *are* the contract:

| To change… | Edit | Takes effect after |
|---|---|---|
| envelope, field names, which columns, ordering | `backend/api/src/repositories/bioactivity.py` | API redeploy (§8.1) |
| add/remove a column on a view | `backend/db/src/models/views.py` | ETL reload (§8.2) |
| structure of `potency_summary` / `measurements` items | `backend/db/src/etl/materializer_bioactivity.py` | ETL reload (§8.2) |
| endpoint path / params / new endpoint | `backend/api/src/routes/{bioactivity,chemical,food}.py` | API redeploy (§8.1) |

- The `name`/`id` field names come from SQL aliases (e.g. `chemical_name AS name`) in the repository.
- The `{data, metadata:{row_count}}` envelope is built in each repository function.
- The nested `measurements`/`potency_summary` item shapes are built in `materializer_bioactivity.py::_aggregate`.

---

## 8. Changing the backend & redeploying staging

All deploys use a **dedicated staging tag and prefix** so production can never be affected:
**image tag `bioactivity`** (prod uses pinned SHAs), **S3 prefix `outputs/staging-bioactivity/`**
(prod uses `LATEST`), and you only ever deploy the **`-Staging`** stacks. Set these once:

```bash
cd foodatlas
aws sso login --profile <your-profile> && export AWS_PROFILE=<your-profile> AWS_REGION=us-west-1
ECR=030635937737.dkr.ecr.us-west-1.amazonaws.com
KGC_BUCKET=foodatlasstoragestack-kgcbucket77ae180e-fu9ow2rwr4u1
CLUSTER=FoodAtlasApiStack-Staging-ApiCluster7CE9CBE6-BtVAz1IpCkmx
SERVICE=FoodAtlasApiStack-Staging-ApiService199661B5-QAbwy3lSlebq
aws ecr get-login-password | docker login --username AWS --password-stdin $ECR
```

### 8.1 You changed **API** code (routes / repositories / config)
Rebuild the API image, push it, and force the staging service to pull it:
```bash
docker build -t $ECR/foodatlas-api:bioactivity backend/api
docker push  $ECR/foodatlas-api:bioactivity
aws ecs update-service --cluster $CLUSTER --service $SERVICE --force-new-deployment
```
> Because the tag (`bioactivity`) is unchanged, `cdk deploy` sees no diff — `--force-new-deployment`
> is what makes the running tasks pull the new image. Rollout takes ~2–3 min.

### 8.2 You changed **DB** code (`models/views.py`, `materializer_*`, models)
Rebuild the DB image and **re-run the ETL** (it drops & recreates the schema, reloads, and
rebuilds the views — a one-off Fargate task pulls the fresh image automatically):
```bash
docker build -t $ECR/foodatlas-db:bioactivity backend/db
docker push  $ECR/foodatlas-db:bioactivity

cd infra/aws/scripts
source ./_lib.sh                          # sets REGION + helpers
STACK=FoodAtlasJobsStack-Staging          # override: target the staging jobs stack
run_jobs_task \
  "[\"python\",\"main.py\",\"load\",\"--parquet-dir\",\"s3://$KGC_BUCKET/outputs/staging-bioactivity/kg/\"]" \
  "staging reload"
cd ../../..
```
`run_jobs_task` launches the task, polls to completion, and tails CloudWatch logs.

### 8.3 You changed the **KG / data** (`backend/kgc`)
Regenerate the KG locally, push the parquet to the **staging** prefix, then reload (§8.2):
```bash
# (regenerate backend/kgc/outputs/kg via the kgc pipeline first)
aws s3 sync backend/kgc/outputs/kg/ s3://$KGC_BUCKET/outputs/staging-bioactivity/kg/ --only-show-errors
# then re-run the ETL as in 8.2
```
> Never sync bioactivity output to `outputs/LATEST` — that's what a **production** load reads.

### 8.4 You changed CDK infra (CORS, instance size, env vars, new resources)
```bash
cd infra/aws
npx cdk diff   'FoodAtlas*-Staging' --context api_image_tag=bioactivity --context db_image_tag=bioactivity
npx cdk deploy 'FoodAtlasApiStack-Staging' --exclusively \
  --context api_image_tag=bioactivity --context db_image_tag=bioactivity --require-approval never
```
- Add a CORS origin: edit `API_CORS_ORIGINS` in `stacks/api_stack.py`, then deploy as above.
- `--exclusively` keeps CDK from touching dependency stacks; always pass the two `*_image_tag` contexts so it deploys the `bioactivity` images, not `latest`.

### Safety checklist (every redeploy)
- [ ] Image tag is **`bioactivity`**, never `latest`.
- [ ] S3 target is **`outputs/staging-bioactivity/`**, never `LATEST`.
- [ ] You deployed only **`*-Staging`** stacks / forced only the **staging** service.

---

## 9. Going live (production)

The API contract is **identical** between staging and production, so your frontend code
doesn't change at launch — only the URL/key the production build points at (already
configured). The launch sequence:

1. **Merge** the bioactivity backend (KGC + DB + API code) to `main`.
2. **Production data load** — the normal weekly update regenerates the KG, syncs to
   `outputs/LATEST`, and runs the prod ETL → loads bioactivity into the **production** RDS.
3. **Production API deploy** — build + push the API image under prod's pinned tag and deploy
   `FoodAtlasApiStack` (no `-Staging`). The `/bioactivity/*` routes go live.
4. **Frontend ships** — your bioactivity pages deploy to the live site.

**Production differs from staging only in config:** prod stacks (no `-Staging`), prod secrets
(no `-staging`), pinned SHA image tags (not `bioactivity`), and `outputs/LATEST` (not the
staging prefix). Same code, same schema, same endpoints.

**Optional:** gate the bioactivity UI behind a **feature flag** so everything deploys quietly
first, then flips on for the public.

---

## 10. Phase 2 (later): bioactivity–disease

**Not in this release.** A future phase will add **chemical ↔ disease** associations *inferred*
from shared lab assays (a chemical active in an assay tied to a disease's marker/mechanism or
therapeutic target). For the frontend it would surface as extra evidence on existing
chemical–disease pages and possibly a chemical → target → disease "mechanism" view. No
disease endpoints/data/UI exist today; the raw disease data is staged on the backend and
turned into associations in a separate update once the inference rules are confirmed.

---

## 11. Reference & troubleshooting

### Resource IDs (staging)
- **API URL / Internal API key:** not hardcoded — fetch via §3 → *Getting access & credentials* (staging-only, rotatable)
- **RDS:** `foodatlasdatabasestack-st-postgresinstance19cdd68a-qj5oikr04ga8` (private subnet — reach it only via the Fargate ETL task, not from your laptop)
- **ECS cluster / service:** `FoodAtlasApiStack-Staging-ApiCluster7CE9CBE6-BtVAz1IpCkmx` / `FoodAtlasApiStack-Staging-ApiService199661B5-QAbwy3lSlebq`
- **ECR:** `030635937737.dkr.ecr.us-west-1.amazonaws.com/{foodatlas-api,foodatlas-db}:bioactivity`
- **KGC bucket / prefix:** `foodatlasstoragestack-kgcbucket77ae180e-fu9ow2rwr4u1` → `outputs/staging-bioactivity/kg/`
- **AWS:** account `030635937737`, region `us-west-1`, SSO profile `<your-profile>` (from the maintainer)

### Command cheat sheet
```bash
# tail staging API logs
aws logs tail FoodAtlasApiStack-Staging-ApiLogGroup1DEDFC07-fHIZBBY5OZDu --follow
# tail the staging ETL logs
aws logs tail "$(aws cloudformation describe-stacks --stack-name FoodAtlasJobsStack-Staging \
  --query "Stacks[0].Outputs[?OutputKey=='JobsLogGroupName'].OutputValue" --output text)" --since 30m
# restart the staging API (pull newest :bioactivity image)
aws ecs update-service --cluster $CLUSTER --service $SERVICE --force-new-deployment
# tear down / rebuild staging
cd infra/aws && npx cdk destroy 'FoodAtlas*-Staging'
```

### Troubleshooting
| Symptom | Cause / fix |
|---|---|
| `401 Invalid API key` | Missing/incorrect `Authorization: Bearer <key>`. Locally, set `API_DEBUG=True` to skip auth. |
| Browser CORS error | Your origin isn't allowed — add it in `stacks/api_stack.py` `API_CORS_ORIGINS` and redeploy (§8.4). |
| Endpoint returns `{"data": [], "row_count": 0}` | Term not found, or the RDS wasn't (re)loaded — re-run the ETL (§8.2). |
| `jq: libonig.so.5` error | `jq` is broken in some shells; the tester script uses Python instead — no action needed. |
| API change didn't show up | You pushed the image but didn't `--force-new-deployment` (§8.1). |
| DB/view change didn't show up | Views are built at load time — re-run the ETL (§8.2). |
| `aws` calls fail with auth errors | `aws sso login --profile <your-profile>` (token expired). |

### What we need from you
1. Your frontend origin(s) for CORS if not `localhost:3000/3001`.
2. Feedback on the response shapes (the example JSON) before we lock them for production.
3. A heads-up before you redeploy staging, so we don't step on each other.
