# FoodAtlas Backend

The backend is organized into four independent Python sub-projects, each with its own dependencies and test suite.

## Sub-projects

| Directory | Description | Production deploy |
|-----------|-------------|-------------------|
| [`api/`](api/) | FastAPI REST service (port 8000) | Docker image → ECS Fargate service (FoodAtlasApiStack) |
| [`db/`](db/) | PostgreSQL schema (SQLAlchemy) and ETL loader | Docker image → ECS Fargate one-off task (FoodAtlasJobsStack) |
| [`ie/`](ie/) | Literature → triplet extraction (PubMed/PMC + BioBERT + OpenAI batch) | Local only; output TSVs feed into KGC's `ie` stage |
| [`kgc/`](kgc/) | Knowledge graph construction pipeline | Local; outputs synced to S3 (FoodAtlasStorageStack) |

`ie` and `kgc` are not deployed as services — they run on a developer machine on a roughly monthly cadence (see [`infra/local/scripts/run_monthly.sh`](../infra/local/scripts/run_monthly.sh)) and publish their parquet outputs to S3 for the `db` loader to consume. For the production deploy targets (CDK stacks, Dockerfiles, push scripts), see [`infra/README.md`](../infra/README.md).

## Getting Started

Each sub-project is a standalone Python package managed by [uv](https://docs.astral.sh/uv/). To work on one:

```bash
cd backend/<project>
uv sync
```

See the root [README](../README.md) for full local stack setup (Docker → DB → API → frontend).

## Running Tests

```bash
cd backend/<project>
uv run pytest
```
