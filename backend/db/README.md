# FoodAtlas DB

PostgreSQL database layer: schema migrations, ORM models, and ETL pipeline for loading KGC output.

## Getting Started

Requires a running PostgreSQL instance (see root README for Docker setup).

```bash
uv sync

# Run migrations
uv run alembic upgrade head

# Load KGC parquet output into the database
uv run python main.py load --parquet-dir /path/to/kgc/outputs
```

## Configuration

Settings are read from `DB_*` environment variables (or a `.env` file):

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `foodatlas` | Database name |
| `DB_USER` | `foodatlas` | Database user |
| `DB_PASSWORD` | `foodatlas` | Database password |

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

## Project Structure

```
db/
├── main.py                 # Click CLI entry point
├── pyproject.toml
├── alembic.ini             # Alembic configuration
├── migrations/
│   ├── env.py              # Alembic environment
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
│       ├── materializer.py
│       ├── materializer_correlation.py
│       └── materializer_search.py
└── tests/
```

## Running Tests

```bash
uv run pytest
```
