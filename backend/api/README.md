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
| `API_CORS_ORIGINS` | `http://localhost:3000` | env var on the task | Comma-separated allowed origins (Vercel domains in prod) |
| `API_DEBUG` | `true` | `False` | Skip API key verification when true |
| `DB_HOST` | `localhost` | RDS endpoint | PostgreSQL host |
| `DB_PORT` | `5432` | RDS endpoint port | PostgreSQL port |
| `DB_NAME` | `foodatlas` | `foodatlas` | Database name |
| `DB_USER` | `foodatlas` | Secrets Manager | Database user |
| `DB_PASSWORD` | `foodatlas` | Secrets Manager | Database password |
| `KGC_BUCKET` | (unset) | `KgcBucketName` output | S3 bucket for parquet artifacts |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/food/metadata` | Food entity metadata |
| GET | `/food/profile` | Macro and micro nutrient profile |
| GET | `/food/composition` | Food-chemical composition (paginated) |
| GET | `/chemical/metadata` | Chemical entity metadata |
| GET | `/chemical/composition` | Foods containing a chemical |
| GET | `/disease/metadata` | Disease entity metadata |
| GET | `/disease/correlation` | Chemical-disease correlations (paginated) |
| GET | `/metadata/statistics` | Database statistics |
| GET | `/metadata/search` | Autocomplete search |
| GET | `/download` | Download entries |

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
│   ├── routes/             # Route handlers
│   │   ├── food.py
│   │   ├── chemical.py
│   │   ├── disease.py
│   │   ├── metadata.py
│   │   └── download.py
│   ├── repositories/       # Database query logic
│   │   ├── food.py
│   │   ├── chemical.py
│   │   ├── disease.py
│   │   └── search.py
│   └── schemas/            # Pydantic response models
└── tests/
```

## Running Tests

```bash
uv run pytest
```
