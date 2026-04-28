# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FoodAtlas is a monorepo containing frontend, backend, and infrastructure code for a food knowledge graph platform.

## Architecture

This is a polyglot monorepo with independent sub-projects:

- **`backend/`** — Four independent Python sub-projects, each with its own `pyproject.toml`, `uv.lock`, and `.venv`:
  - `api/` — FastAPI REST service (uvicorn, port 8000). Routes, repositories, config, auth.
  - `db/` — PostgreSQL schema (SQLAlchemy models, drop-and-recreate on each load), ETL pipeline (parquet → Postgres).
  - `ie/` — Information extraction pipeline (Click CLI, four stages: corpus → search → filtering → extraction). PubMed/PMC search + BioBERT filter + OpenAI batch extraction.
  - `kgc/` — Knowledge graph construction pipeline (Click CLI, five stages: ingest → entities → triplets → ie → enrichment).
- **`frontend/`** — Next.js 14 app (React 18, TypeScript, Tailwind, App Router). Dev server runs on port 3001.
- **`infra/`** — Local dev infrastructure (`infra/local/`, Docker Compose Postgres 16) and AWS CDK (`infra/aws/`).
- **`docs/`** — Architecture and planning documents.

Each backend sub-project follows the same layout: `src/` is the module, `tests/` for tests, `main.py` as entry point.

### Local stack

PostgreSQL 16 (Docker, port 5432) → DB ETL (drops and recreates schema, loads from KGC parquet) → FastAPI (port 8000) → Next.js (port 3001)

### Configuration split

- **Root `pyproject.toml`** — Shared linter/checker configs (ruff, mypy, bandit). NOT a Python package.
- **Per-project `pyproject.toml`** — Build, dependencies, and pytest config only.
- **Root `.pre-commit-config.yaml`** — All git hooks. Uses official pre-commit repos for Python tools; local hooks for ESLint and pytest.

## Commands

### Local dev setup (full stack)

```bash
# Start PostgreSQL
docker compose -f infra/local/docker-compose.yml up -d

# Load KGC data into PostgreSQL (drops and recreates schema)
cd backend/db && uv run python main.py load

# Start API server (port 8000)
cd backend/api && uv run python main.py

# Start frontend (port 3001)
cd frontend && npm run dev
```

### Backend commands

All backend work must be done from within the sub-project directory:

```bash
# Install dependencies for a sub-project
cd backend/api && uv sync

# Run a sub-project
cd backend/api && uv run python main.py

# Run tests for a sub-project
cd backend/api && uv run pytest

# Run a single test
cd backend/api && uv run pytest tests/test_example.py::test_version

# Set up git hooks (from repo root)
./scripts/setup-git-hooks.sh
```

### Frontend commands

```bash
# Install dependencies
cd frontend && npm ci

# Run dev server
cd frontend && npm run dev

# Build for production
cd frontend && npm run build

# Run tests
cd frontend && npm test

# Lint (ESLint + TypeScript type check)
cd frontend && npm run lint
```

## Code Standards

- **Python**: 3.12+ (managed by uv)
- **Ruff**: linting + formatting (E, W, F, I, B, C4, UP, ARG, SIM, TCH, PTH, ERA, PL, RUF rules)
- **MyPy**: strict mode with `explicit_package_bases` for monorepo support
- **Bandit**: security scanning (excludes tests, skips B101/B311)
- **Pytest**: 80% coverage minimum per sub-project (`--cov-fail-under=80`)
- **TypeScript**: strict mode, Next.js 14 with App Router
- **ESLint**: next/core-web-vitals config, zero warnings
- **Vitest**: test framework with React Testing Library

## Git Hooks

Pre-commit hooks run automatically — do not run linters manually unless debugging:
- **On commit**: ruff, ruff-format, bandit, mypy (scoped to `backend/`); ESLint (scoped to `frontend/`)
- **On push**: pytest per sub-project (only runs if files in that sub-project changed)

## General Rules

1. **File size limit**: Do not allow code files to exceed 300 lines. Refactor by splitting into smaller modules.
2. **No lazy bypasses**: Do not use `# noqa`, `# type: ignore` to bypass errors. Fix the underlying issue.
3. **No cheating on test coverage**: Do not lower `--cov-fail-under` threshold or add files to `[tool.coverage.run] omit`. Write proper tests instead.
4. **Rely on pre-commit hooks**: Only run checks manually when debugging.

## ETL Notes

- **COPY text escaping**: PostgreSQL COPY text format applies backslash unescaping before type parsing. Array (`TEXT[]`) and JSONB fields need two-level escaping: type-level first, then COPY-level (doubling backslashes, escaping `\n`/`\t`/`\r`).
- **Entity dedup**: The entity registry can map multiple `(source, native_id)` pairs to the same `foodatlas_id` via merges/aliases. Entity creation functions must check `store._entities.index` before appending.
- **Triplet dedup**: `_insert_or_merge` computes `existing_mask` once — intra-batch duplicate keys all pass as "new". The `new_df` must be deduplicated before concat, and attestation accumulation must use `setdefault`+`append` (not overwrite).
