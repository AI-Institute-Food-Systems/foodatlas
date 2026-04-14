# FoodAtlas Backend

The backend is organized into four independent Python sub-projects, each with its own dependencies and test suite.

## Sub-projects

| Directory | Description | Production deploy | Status |
|-----------|-------------|-------------------|--------|
| [`api/`](api/) | FastAPI REST service (port 8000) | Docker image → ECS Fargate service (FoodAtlasApiStack) | Active |
| [`db/`](db/) | PostgreSQL schema, migrations, and ETL | Docker image → ECS Fargate one-off task (FoodAtlasJobsStack) | Active |
| [`ie/`](ie/) | Information extraction (LLM + BioBERT) | Local only | Active |
| [`kgc/`](kgc/) | Knowledge graph construction pipeline | Local; outputs synced to S3 (FoodAtlasStorageStack) | Active |

For the production deploy targets (CDK stacks, Dockerfiles, push scripts), see [`infra/README.md`](../infra/README.md).

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
