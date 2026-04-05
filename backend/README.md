# FoodAtlas Backend

The backend is organized into four independent Python sub-projects, each with its own dependencies and test suite.

## Sub-projects

| Directory | Description | Status |
|-----------|-------------|--------|
| [`api/`](api/) | FastAPI REST service (port 8000) | Active |
| [`db/`](db/) | PostgreSQL schema, migrations, and ETL | Active |
| [`ie/`](ie/) | Information extraction | Stub |
| [`kgc/`](kgc/) | Knowledge graph construction pipeline | Active |

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
