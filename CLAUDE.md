# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FoodAtlas is a monorepo containing frontend, backend, and infrastructure code for a food knowledge graph platform.

## Architecture

This is a polyglot monorepo with independent sub-projects:

- **`backend/`** — Four independent Python sub-projects, each with its own `pyproject.toml`, `uv.lock`, and `.venv`:
  - `api/` — API service
  - `db/` — Database layer
  - `ie/` — Information extraction
  - `kgc/` — Knowledge graph construction
- **`frontend/`** — Next.js app (not yet initialized)
- **`infra/`** — AWS CDK infrastructure (Python, not yet initialized)
Each backend sub-project follows the same layout: `src/__init__.py` is the module, `tests/` for tests, `main.py` as entry point.

### Configuration split

- **Root `pyproject.toml`** — Shared linter/checker configs (ruff, mypy, bandit). NOT a Python package.
- **Per-project `pyproject.toml`** — Build, dependencies, and pytest config only.
- **Root `.pre-commit-config.yaml`** — All git hooks. Uses official pre-commit repos (not local/uvx).

## Commands

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

## Code Standards

- **Python**: 3.12+ required
- **Ruff**: linting + formatting (E, W, F, I, B, C4, UP, ARG, SIM, TCH, PTH, ERA, PL, RUF rules)
- **MyPy**: strict mode with `explicit_package_bases` for monorepo support
- **Bandit**: security scanning (excludes tests, skips B101/B311)
- **Pytest**: 80% coverage minimum per sub-project (`--cov-fail-under=80`)

## Git Hooks

Pre-commit hooks run automatically — do not run linters manually unless debugging:
- **On commit**: ruff, ruff-format, bandit, mypy (scoped to `backend/`)
- **On push**: pytest per sub-project (only runs if files in that sub-project changed)

## General Rules

1. **File size limit**: Do not allow code files to exceed 300 lines. Refactor by splitting into smaller modules.
2. **No lazy bypasses**: Do not use `# noqa`, `# type: ignore` to bypass errors. Fix the underlying issue.
3. **No cheating on test coverage**: Do not lower `--cov-fail-under` threshold or add files to `[tool.coverage.run] omit`. Write proper tests instead.
4. **Rely on pre-commit hooks**: Only run checks manually when debugging.
