# FoodAtlas API

FastAPI REST service for the FoodAtlas knowledge graph.

## Getting Started (local)

Requires a running PostgreSQL instance (see [root README](../../README.md) for Docker setup).

```bash
uv sync
uv run python main.py
```

The server starts at `http://localhost:8000` with auto-reload enabled.

## Production

In AWS, this code runs as a containerized ECS Fargate service behind an ALB (`FoodAtlasApiStack`). The image is built from [`Dockerfile`](Dockerfile) and pushed via [`scripts/push-to-ecr.sh`](scripts/push-to-ecr.sh) to the `foodatlas-api` ECR repo. ECS injects `DB_USER` and `DB_PASSWORD` from AWS Secrets Manager at task startup; `DB_HOST` points at the RDS endpoint; `API_DEBUG` is `False` so the bearer-token check is enforced.

The `/health` endpoint is used by the ALB target group health check — it must return `200` for the task to be considered healthy.

See [`infra/README.md`](../../infra/README.md) for the full production deploy guide.

## Configuration

Settings are read from `API_*` and `DB_*` environment variables (or a `.env` file in local mode):

| Variable | Default (local) | Production source | Description |
|---|---|---|---|
| `API_KEY` | (empty) | env var on the task | Bearer token for authentication |
| `API_CORS_ORIGINS` | `http://localhost:3000` | env var on the task | Comma-separated allowed origins. Set to `http://localhost:3001` to call from the local Next.js dev server. |
| `API_DEBUG` | `true` | `False` | Skip API key verification when true |
| `API_DOWNLOADS_BUCKET` | (empty) | downloads bucket name from `FoodAtlasDownloadsStack` | S3 bucket holding `bundles/index.json` (public) and the bundle zips (private; the task role pre-signs them) |
| `API_DOWNLOADS_REGION` | `us-west-1` | env var on the task | AWS region for the downloads bucket |
| `API_DOWNLOADS_PRESIGN_SECONDS` | `600` | default | Lifetime of the pre-signed S3 URL that `/v1/bundles/{version}/download` redirects to. Bundle zips are private; this URL is the only unauthenticated path to one. |
| `API_PUBLIC_URL` | `https://api.foodatlas.ai` | default | Base for the gated `download_link` values in `/v1/bundles` |
| `API_UMAMI_WEBSITE_ID` | (empty = off) | CDK context `api_umami_website_id` | umami website that receives one `api_request` event per external `/v1` call (`src/umami_sink.py`). Not gated on `API_DEBUG`, so a local run can post to a fake receiver. |
| `API_UMAMI_HOST_URL` | `https://umami.aifs.ucdavis.edu` | CDK context `api_umami_host_url` (optional) | umami base URL; events go to `<host>/api/send` |
| `API_UMAMI_HOSTNAME` | `api.foodatlas.ai` | default | `hostname` field on each event |
| `API_UMAMI_SAMPLE_RATE` | `1.0` | default (edit the task def to throttle) | Fraction of events sent; the knob if umami's VM struggles |
| `API_UMAMI_QUEUE_SIZE` | `1000` | default | Bounded send queue; events beyond it are dropped, never awaited |
| `DB_HOST` | `localhost` | RDS endpoint | PostgreSQL host |
| `DB_PORT` | `5432` | RDS endpoint port | PostgreSQL port |
| `DB_NAME` | `foodatlas` | `foodatlas` | Database name |
| `DB_USER` | `foodatlas` | Secrets Manager | Database user |
| `DB_PASSWORD` | `foodatlas` | Secrets Manager | Database password |

## API Endpoints

All routes (except `/health`) require the `Authorization: Bearer <API_KEY>` header when `API_DEBUG=False`.

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness probe (no auth, used by ALB target group) |
| GET | `/food/metadata` | Food entity metadata |
| GET | `/food/taxonomy` | Food's FoodOn ancestor chain |
| GET | `/food/profile` | Macro and micro nutrient profile |
| GET | `/food/composition` | Food-chemical composition (paginated) |
| GET | `/food/composition/counts` | Composition row counts per chemical group (used to drive composition filters) |
| GET | `/chemical/metadata` | Chemical entity metadata |
| GET | `/chemical/taxonomy` | Chemical's ChEBI ancestor chain |
| GET | `/chemical/composition` | Foods containing a chemical |
| GET | `/chemical/correlation` | Diseases correlated with a chemical |
| GET | `/disease/metadata` | Disease entity metadata |
| GET | `/disease/taxonomy` | Disease's MeSH ancestor chain |
| GET | `/disease/correlation` | Chemicals correlated with a disease (paginated) |
| GET | `/metadata/statistics` | Database statistics |
| GET | `/metadata/search` | Autocomplete search (trigram-backed) |
| GET | `/resolve` | Resolve a `foodatlas_id` to `entity_type` + `common_name` (used by frontend URL routing) |
| GET | `/download` | List released data bundles from the public downloads bucket |

## Project Structure

```
api/
├── main.py                 # Uvicorn entry point
├── Dockerfile              # Multi-stage build for the foodatlas-api image
├── .dockerignore
├── pyproject.toml
├── scripts/
│   └── push-to-ecr.sh      # Build + push to foodatlas-api ECR repo
├── src/
│   ├── app.py              # FastAPI application factory + /health
│   ├── config.py           # APISettings (pydantic-settings)
│   ├── dependencies.py     # DB session and auth dependencies
│   ├── routes/             # Route handlers (food, chemical, disease, metadata, download, resolve)
│   ├── repositories/       # Database query logic + result formatting + downloads/manifest fetcher
│   └── schemas/            # Pydantic response models
└── tests/
```

## Running Tests

```bash
uv run pytest
```
