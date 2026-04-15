# FoodAtlas DB

PostgreSQL database layer: schema migrations, ORM models, and ETL pipeline for loading KGC output.

## Getting Started (local)

Requires a running PostgreSQL instance (see [root README](../../README.md) for Docker setup).

```bash
uv sync

# Run migrations
uv run alembic upgrade head

# Load KGC parquet output into the database
uv run python main.py load

# Rebuild materialized views from existing base tables (skip parquet
# read + base inserts). Use when iterating on materializer logic.
uv run python main.py refresh
```

## Production

In AWS, this code runs as a one-off ECS Fargate task — invoked on demand for migrations and ETL data loads, never as a long-running service. The image is built from [`Dockerfile`](Dockerfile), pushed via [`scripts/push-to-ecr.sh`](scripts/push-to-ecr.sh) to the `foodatlas-db` ECR repo, and registered as a task definition by `FoodAtlasJobsStack`.

```bash
# From repo root, run a migration against RDS
infra/cdk/scripts/run-migration.sh

# Load data from a specific KGC version published to S3
infra/cdk/scripts/run-data-load.sh 20260413T221503Z
```

`DB_USER` and `DB_PASSWORD` are injected from AWS Secrets Manager at task startup; `DB_HOST` points at the RDS endpoint. The loader auto-detects `s3://` URIs and downloads to the container's ephemeral storage before reading. See [`infra/README.md`](../../infra/README.md) for the full production deploy guide.

## Migration Workflow

We use Alembic with **incremental migrations** — each schema change becomes a new version file. Don't edit `001_initial_schema.py` after it's been deployed; create `002_*.py`, `003_*.py`, etc.

```bash
# 1. Update the SQLAlchemy model
vim src/models/entities.py

# 2. Autogenerate a migration from the model diff
uv run alembic revision --autogenerate -m "add my_column to entities"

# 3. Review and edit the generated 002_*.py — autogen is not perfect
#    (it can miss renames, can't generate data backfills, etc.)

# 4. Test against local Postgres
uv run alembic upgrade head

# 5. In production: rebuild the foodatlas-db image and run the migration
#    via infra/cdk/scripts/run-migration.sh — see infra/README.md
```

The migration must run **before** the API code that depends on the new schema. Reverse order causes 500s on the production API.

## Configuration

Settings are read from `DB_*` environment variables (or a `.env` file in local mode):

| Variable | Default (local) | Production source | Description |
|---|---|---|---|
| `DB_HOST` | `localhost` | RDS endpoint | PostgreSQL host |
| `DB_PORT` | `5432` | RDS endpoint port | PostgreSQL port |
| `DB_NAME` | `foodatlas` | `foodatlas` | Database name |
| `DB_USER` | `foodatlas` | Secrets Manager | Database user |
| `DB_PASSWORD` | `foodatlas` | Secrets Manager | Database password |
| `KGC_BUCKET` | (unset) | `KgcBucketName` output | S3 bucket the loader reads from |

## Schema

The database has two layers:

- **Base tables** — Normalized data loaded directly from KGC parquet files:
  `base_entities`, `relationships`, `base_triplets`, `base_evidence`, `base_attestations`
- **Materialized API tables** — Pre-joined, denormalized tables optimized for API queries:
  `mv_food_entities`, `mv_chemical_entities`, `mv_disease_entities`,
  `mv_food_chemical_composition`, `mv_chemical_disease_correlation`,
  `mv_search_auto_complete`, `mv_metadata_statistics`

Migrations are managed by Alembic (see `alembic.ini` and `migrations/`).

## ETL Pipeline

The `load` CLI command reads KGC parquet output and populates the database:

1. **Load** base tables from parquet files (entities, relationships, triplets, evidence, attestations)
2. **Materialize** API tables by joining and denormalizing base data
3. **Build** search indexes (trigram-based autocomplete)

The `refresh` CLI command re-runs steps 2–3 against the existing base tables
without touching parquet. Use this when iterating on materializer logic.

## Project Structure

```
db/
├── main.py                 # Click CLI entry point (load, refresh)
├── Dockerfile              # Multi-stage build for the foodatlas-db image
├── .dockerignore
├── pyproject.toml
├── alembic.ini             # Alembic configuration
├── scripts/
│   └── push-to-ecr.sh      # Build + push to foodatlas-db ECR repo
├── migrations/
│   ├── env.py              # Alembic environment (reads DBSettings)
│   └── versions/
│       └── 001_initial_schema.py
├── src/
│   ├── config.py           # DBSettings (pydantic-settings)
│   ├── engine.py           # SQLAlchemy engine factory
│   ├── models/             # SQLAlchemy ORM models
│   │   ├── base.py
│   │   ├── entities.py
│   │   ├── relationships.py
│   │   ├── triplets.py
│   │   ├── attestations.py
│   │   ├── evidence.py
│   │   └── views.py        # Materialized API table models
│   └── etl/                # ETL pipeline
│       ├── parquet_reader.py
│       ├── bulk_insert.py
│       ├── loader.py       # Orchestrates load
│       ├── s3_sync.py      # Downloads s3:// parquet to a local temp dir
│       ├── materializer.py
│       ├── materializer_correlation.py
│       └── materializer_search.py
└── tests/
```

## Running Tests

```bash
uv run pytest
```
