# Contributing to FoodAtlas

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) — Python package manager
- [pre-commit](https://pre-commit.com/) — Git hook framework

## Setup

```bash
git clone https://github.com/AI-Institute-Food-Systems/foodatlas.git
cd foodatlas
./scripts/setup-git-hooks.sh
```

Install dependencies for the sub-project you're working on:

```bash
cd backend/api && uv sync
```

## Branching Strategy

```
main           ← protected, production-ready
  ├── hotfix/  ← urgent fixes, PR directly to main
  └── dev      ← integration branch, PR target for features
       └── feat/your-name/description
       └── fix/your-name/description
```

- **Never push directly to `main` or `dev`.**
- Create feature/fix branches off `dev`. Open PRs targeting `dev`.
- For urgent production fixes, create `hotfix/<description>` branches off `main` and PR directly to `main`.
- Use the naming convention: `feat/<name>/<description>`, `fix/<name>/<description>`, or `hotfix/<description>`.

## Development Workflow

1. Create a branch off `dev`:
   ```bash
   git checkout dev && git pull
   git checkout -b feat/your-name/short-description
   ```

2. Work inside a backend sub-project:
   ```bash
   cd backend/api
   uv sync
   uv run python main.py      # run
   uv run pytest              # test
   ```

3. Commit and push. Pre-commit hooks handle linting automatically.

4. Open a PR targeting `dev`.

## Project Structure

Each backend sub-project (`api`, `db`, `ie`, `kgc`) is independent with its own:
- `pyproject.toml` — dependencies and pytest config
- `uv.lock` — locked dependencies
- `src/` — source code
- `tests/` — tests
- `main.py` — entry point

Shared tool configs (ruff, mypy, bandit) live in the root `pyproject.toml`.

## Code Quality

### Automated Checks (Git Hooks)

On **commit**, pre-commit hooks run:
- **Ruff** — linting and formatting
- **Bandit** — security scanning
- **Mypy** — type checking
- YAML/TOML/JSON validation, trailing whitespace, etc.

On **push**, pre-push hooks run:
- **Pytest** — per sub-project (only if files in that sub-project changed)

### CI Pipeline

PRs to `dev` trigger GitHub Actions CI, which runs:

| Job | What it does |
|-----|-------------|
| **lint** | Ruff lint + format check |
| **security** | Bandit scan |
| **typecheck** | Mypy type check |
| **test** | Pytest per sub-project (only changed projects) |

CI only runs when `backend/` files are changed. All checks must pass before merging.

### Standards

- **80% test coverage minimum** per sub-project (enforced by pytest)
- **No `# noqa` or `# type: ignore`** — fix the underlying issue
- **300-line file limit** — refactor into smaller modules if exceeded
- **Do not lower coverage thresholds** or add files to coverage omit lists

## Running Checks Manually

You generally don't need to — git hooks handle this. For debugging:

```bash
# Lint
ruff check backend/
ruff format --check backend/

# Security
bandit -c pyproject.toml -r backend/ --exclude "**/tests"

# Type check
mypy --explicit-package-bases backend/

# Tests (from sub-project dir)
cd backend/api && uv run pytest
```

## Adding Dependencies

```bash
cd backend/api
uv add <package>          # runtime dependency
uv add --group dev <package>  # dev dependency
```

This updates both `pyproject.toml` and `uv.lock`. Commit both files.
