# Developing FoodAtlas

This document is the technical guide for working on the codebase: getting the stack running locally, the dev workflow, hooks, CI, and code standards.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) — runs the local PostgreSQL container
- [uv](https://docs.astral.sh/uv/) — manages Python and Python dependencies (auto-installed by the setup script)
- Node.js 20+ and npm — for the Next.js frontend (install separately)

## First-time setup

```bash
git clone https://github.com/AI-Institute-Food-Systems/foodatlas.git
cd foodatlas
./scripts/check-prereqs.sh
```

`check-prereqs.sh` auto-installs uv, wires up git hooks, runs `uv sync` in every backend sub-project, and runs `npm ci` in `frontend/`. Run it whenever you suspect your local setup has drifted.

## Running the local stack

The local stack is **PostgreSQL → DB load → FastAPI → Next.js**, all on one machine.

### 1. Start PostgreSQL

```bash
docker compose -f infra/local/docker-compose.yml up -d
```

PostgreSQL 16 listens on port 5432, database `foodatlas` (user/password: `foodatlas`/`foodatlas`). The init script enables `pg_trgm`, which the search autocomplete index depends on. Credentials are committed deliberately — they only work against your local container.

### 2. Load knowledge graph data

```bash
cd backend/db
uv run python main.py load
```

Drops and recreates the schema from the SQLAlchemy models, then loads KGC parquet output. See [`backend/kgc/README.md`](backend/kgc/README.md) to generate the parquet locally, or [`infra/README.md`](infra/README.md) to pull a published version from S3.

### 3. Start the API

```bash
cd backend/api
uv run python main.py
```

FastAPI runs at `http://localhost:8000`. In debug mode (default), bearer-token auth is skipped.

### 4. Start the frontend

```bash
cd frontend
npm run dev
```

Next.js runs at `http://localhost:3001` (the dev server is pinned to 3001 in `package.json`). It reads `NEXT_PUBLIC_API_URL` from `frontend/.env.local`; default is `http://localhost:8000`.

> **CORS note.** The API's default `API_CORS_ORIGINS` is `http://localhost:3000`. To call it from the dev frontend on `:3001`, set `API_CORS_ORIGINS=http://localhost:3001` on the API process (in a `.env` or as an env var).

### Remote access (SSH tunnel)

If running on a remote dev box:

```bash
ssh -L 3001:localhost:3001 -L 8000:localhost:8000 user@your-server
```

Then open `http://localhost:3001` in your local browser.

### Environment variables

Each sub-project reads its own `.env`. Defaults work out of the box for local dev.

| Variable                | Default (local)           | Description                                                                      |
| ----------------------- | ------------------------- | -------------------------------------------------------------------------------- |
| `DB_HOST`             | `localhost`             | PostgreSQL host                                                                  |
| `DB_PORT`             | `5432`                  | PostgreSQL port                                                                  |
| `DB_NAME`             | `foodatlas`             | Database name                                                                    |
| `DB_USER`             | `foodatlas`             | Database user                                                                    |
| `DB_PASSWORD`         | `foodatlas`             | Database password                                                                |
| `API_KEY`             | (empty)                   | Bearer token (skipped in debug mode)                                             |
| `API_CORS_ORIGINS`    | `http://localhost:3000` | Comma-separated allowed origins (set to `http://localhost:3001` for local dev) |
| `API_DEBUG`           | `true`                  | Skip API key verification when true                                              |
| `NEXT_PUBLIC_API_URL` | —                        | Backend API URL (set to `http://localhost:8000`)                               |
| `NEXT_PUBLIC_API_KEY` | —                        | Backend API key (not needed in debug mode)                                       |

In production, `DB_USER`/`DB_PASSWORD` come from AWS Secrets Manager, `API_DEBUG=False`, and `NEXT_PUBLIC_API_URL` points at the ALB DNS or a custom domain. See [`infra/README.md`](infra/README.md).

## Branching strategy

```
main           ← protected, production-ready (CD deploys from here)
  └── dev      ← integration branch, only branch allowed to PR into main
       ├── feat/<name>/<description>
       ├── fix/<name>/<description>
       └── hotfix/<description>          ← high priority, but still flows through dev
```

- **Never push directly to `main` or `dev`.**
- Every change — feature, fix, or hotfix — branches off `dev` and PRs into `dev`.
- `main` is updated only via `dev → main` PRs. The `guard-main.yml` workflow enforces this and will fail any PR to `main` whose source isn't `dev`.
- The `hotfix/<description>` name is purely a signal to reviewers that a change is high-priority and should be expedited through dev → main. There's no different mechanical path: it still goes feat-style through dev, gets the same CI, and rides the next dev → main rollup (which can be opened immediately for an urgent fix).
- Naming: `feat/<name>/<description>`, `fix/<name>/<description>`, `hotfix/<description>`.

## Development workflow

1. Branch off `dev`:
   ```bash
   git checkout dev && git pull
   git checkout -b feat/your-name/short-description
   ```
2. Work inside a sub-project (`cd backend/<project>` or `cd frontend`). Each backend sub-project is independent — its own `pyproject.toml`, `uv.lock`, and `.venv`. Shared lint/type/security tool config (ruff, mypy, bandit) lives in the root `pyproject.toml`.
3. Commit and push. Pre-commit hooks handle linting automatically.
4. Open a PR targeting `dev`.

### Merge strategy

Each merge target has a fixed strategy — pick the right one in the GitHub PR UI:

| PR direction | Strategy | Why |
|---|---|---|
| `feat/*`, `fix/*`, or `hotfix/*` → `dev` | **Squash and merge** | Collapses a noisy review history into one logical commit on `dev`. |
| `dev` → `main` | **Create a merge commit** | Preserves the individual squashed-feature commits already on `dev` so production history reflects exactly what shipped. |

### Drift between `dev` and `main`

Because everything reaches `main` only via `dev → main`, `dev` is always a content-superset of `main`: every patch on `main` is also on `dev`. There is no scenario in normal operation where `dev` falls behind `main`.

**Don't be fooled by `git log dev..main`.** Each `dev → main` merge creates a merge-commit shell on `main` only (that's what merge commits *are*). After many rollups, `git log origin/dev..origin/main` will report a growing count of commits that exist on `main` but not on `dev`. **This is not drift.** Those commits are merge-shells whose tree content is already in `dev` via the squashed feature commits they wrap.

The only meaningful drift check is at the file level:

```bash
git fetch origin
git diff origin/dev origin/main             # empty = no drift
git cherry origin/dev origin/main | grep '^+'   # any '+' line = real new content on main
```

If either of these shows real content on `main` that isn't on `dev`, **something bypassed the dev → main rule** — investigate before doing anything else (likely cause: someone disabled the `guard-main` workflow or used admin override). Once you understand what landed, sync it back into `dev` with the recipe below; otherwise leave `dev` alone.

#### Emergency sync-back recipe (should never be needed)

```bash
git checkout main && git pull
git checkout dev && git pull
git checkout -b chore/sync-main-into-dev
git merge main
git push -u origin chore/sync-main-into-dev
```

Then open a PR targeting `dev` and squash-merge it. Past example: [PR #133](https://github.com/AI-Institute-Food-Systems/foodatlas/pull/133).

## Code quality

### Automated checks (git hooks)

On **commit**, pre-commit hooks run:

- **Ruff** — linting and formatting
- **Bandit** — security scanning
- **Mypy** — type checking
- YAML/TOML/JSON validation, trailing whitespace, etc.

On **push**, pre-push hooks run:

- **Pytest** — per sub-project (only if files in that sub-project changed)

### CI pipeline

PRs to `dev` trigger GitHub Actions CI, which runs jobs scoped to what changed:

| Job                  | Triggers when                                                | What it does                                                                       |
| -------------------- | ------------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| `python-lint`      | any `backend/` or `infra/aws/` change                    | Ruff lint + format check                                                           |
| `python-security`  | any `backend/` or `infra/aws/` change                    | Bandit scan                                                                        |
| `python-typecheck` | any `backend/` or `infra/aws/` change                    | Mypy type check                                                                    |
| `backend-test`     | matrix over changed `backend/{api,db,ie,kgc}` sub-projects | Pytest with coverage                                                               |
| `infra-aws-test`   | `infra/aws/` changes                                       | Pytest (CDK snapshot tests)                                                        |
| `frontend-lint`    | `frontend/` changes                                        | ESLint +`tsc --noEmit`                                                           |
| `frontend-build`   | `frontend/` changes                                        | `next build`                                                                     |
| `frontend-test`    | `frontend/` changes                                        | Vitest                                                                             |
| `ci-success`       | always                                                       | Aggregator: passes only if every triggered job passed (skipped jobs are tolerated) |

A `changes` job up front uses `git diff` to set per-project booleans; downstream jobs gate on those. Markdown-only diffs skip every job. `ci-success` is the single required check on PRs.

### Standards

- **80% test coverage minimum** per sub-project (enforced by pytest)
- **No `# noqa` or `# type: ignore`** — fix the underlying issue
- **300-line file limit** — refactor into smaller modules if exceeded
- **Do not lower coverage thresholds** or add files to coverage omit lists

## Running checks manually

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

## Adding dependencies

```bash
cd backend/api
uv add <package>              # runtime dependency
uv add --group dev <package>  # dev dependency
```

This updates both `pyproject.toml` and `uv.lock`. Commit both files.
