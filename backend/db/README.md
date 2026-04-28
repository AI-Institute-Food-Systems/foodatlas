# FoodAtlas DB

PostgreSQL database layer: SQLAlchemy ORM models and ETL pipeline for loading KGC output.

## Getting Started (local)

Requires a running PostgreSQL instance (see [root README](../../README.md) for Docker setup).

```bash
uv sync

# Load KGC parquet output into the database (drops + recreates schema)
uv run python main.py load

# Rebuild materialized views from existing base tables (skip parquet
# read + base inserts). Use when iterating on materializer logic.
uv run python main.py refresh
```

## Production

In AWS, this code runs as a one-off ECS Fargate task — invoked on demand for ETL data loads, never as a long-running service. The image is built from [`Dockerfile`](Dockerfile), pushed via [`scripts/push-to-ecr.sh`](scripts/push-to-ecr.sh) to the `foodatlas-db` ECR repo, and registered as a task definition by `FoodAtlasJobsStack`.

```bash
# Load data from a specific KGC version published to S3
infra/aws/scripts/run-data-load.sh 20260413T221503Z
```

`DB_USER` and `DB_PASSWORD` are injected from AWS Secrets Manager at task startup; `DB_HOST` points at the RDS endpoint. The loader auto-detects `s3://` URIs and downloads to the container's ephemeral storage before reading. See [`infra/README.md`](../../infra/README.md) for the full production deploy guide.

## Schema changes

There are no migrations. The `load` command drops and recreates the entire schema on every run via `Base.metadata.drop_all` → `create_all`, so any model change takes effect the next time the loader runs.

```bash
# 1. Update the SQLAlchemy model
vim src/models/entities.py

# 2. Test locally — drops + recreates + loads from parquet
uv run python main.py load

# 3. In production: rebuild the foodatlas-db image and run
#    infra/aws/scripts/run-data-load.sh against the latest KGC version
```

Brief downtime is expected while the reload runs (~minutes). Trigger the reload **before** rolling out API code that depends on the new schema.

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
- **Materialized API tables** (`mv_*`) — Pre-joined, denormalized tables populated by the materializer step. The API reads only these. Examples: `mv_food_entities`, `mv_chemical_entities`, `mv_disease_entities`, `mv_food_chemical_composition`, `mv_chemical_disease_correlation`, `mv_search_auto_complete`, `mv_metadata_statistics`.

These are SQLAlchemy-managed tables, not Postgres `MATERIALIZED VIEW` objects — the `mv_` prefix is conventional, and the `refresh` command rebuilds them from the base tables.

The autocomplete index (`mv_search_auto_complete`) uses Postgres trigram similarity, so the database needs the `pg_trgm` extension installed. The local Docker init script (`infra/local/init-db.sql`) handles this; in production the loader enables it on first run.

The schema lives entirely in the SQLAlchemy models under `src/models/`. `load` drops and recreates everything, so the models are the only source of truth.

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
├── scripts/
│   └── push-to-ecr.sh      # Build + push to foodatlas-db ECR repo
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
│       ├── loader.py       # Orchestrates load + refresh; ensures pg_trgm extension
│       ├── s3_sync.py      # Downloads s3:// parquet to a local temp dir
│       ├── materializer.py            # Top-level materializer for entity tables
│       ├── materializer_composition.py # mv_food_chemical_composition + counts
│       ├── materializer_correlation.py # mv_chemical_disease_correlation
│       └── materializer_search.py      # mv_search_auto_complete (trigram)
└── tests/
```

## Running Tests

```bash
uv run pytest
```
