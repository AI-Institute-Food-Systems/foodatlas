# FoodAtlas

A food knowledge graph platform. This monorepo contains the frontend, backend services, and infrastructure code.

## Repository Structure

```
.
├── frontend/               # Next.js 14 web app (React 18, TypeScript, Tailwind)
├── backend/
│   ├── api/                # FastAPI REST service (port 8000)
│   ├── db/                 # PostgreSQL schema, migrations, and ETL
│   ├── ie/                 # Information extraction (stub)
│   └── kgc/                # Knowledge graph construction pipeline
├── infra/
│   └── docker-compose.yml  # Local PostgreSQL 16
├── docs/                   # Architecture and planning docs
├── scripts/                # Setup and utility scripts
├── .github/workflows/      # CI/CD pipelines
├── pyproject.toml          # Shared linter/checker configs (ruff, mypy, bandit)
└── .pre-commit-config.yaml # Git hooks
```

## Running Locally

The full stack is: **PostgreSQL → DB migrations & data load → FastAPI → Next.js**.

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (for PostgreSQL)
- [uv](https://docs.astral.sh/uv/) (Python package manager; handles Python installation automatically)
- Node.js 20+
- npm

### 1. Clone and install dependencies

```bash
git clone https://github.com/AI-Institute-Food-Systems/foodatlas.git
cd foodatlas
./scripts/check-prereqs.sh
```

The setup script auto-installs uv, git hooks, backend dependencies, and frontend dependencies. Node.js and npm must be installed separately.

### 2. Start PostgreSQL

```bash
docker compose -f infra/docker-compose.yml up -d
```

This starts a PostgreSQL 16 container on port 5432 with database `foodatlas` (user: `foodatlas`, password: `foodatlas`).

### 3. Run database migrations

```bash
cd backend/db
uv run alembic upgrade head
```

### 4. Load knowledge graph data

```bash
cd backend/db
uv run python main.py load --parquet-dir ../kgc/outputs/kg
```

This loads KGC parquet output into PostgreSQL. See [`backend/kgc/README.md`](backend/kgc/README.md) for how to generate the parquet files.

### 5. Start the API server

```bash
cd backend/api
uv run python main.py
```

The FastAPI server runs at `http://localhost:8000`. In debug mode (default), API key authentication is skipped.

### 6. Start the frontend

```bash
cd frontend
npm run dev
```

Open [http://localhost:3000](http://localhost:3001). The frontend connects to the API at `http://localhost:8000` by default.

### Environment Variables

All services work with defaults for local development. Override via `.env` files or environment variables:

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `foodatlas` | Database name |
| `DB_USER` | `foodatlas` | Database user |
| `DB_PASSWORD` | `foodatlas` | Database password |
| `API_KEY` | (empty) | API key (skipped in debug mode) |
| `API_CORS_ORIGINS` | `http://localhost:3000` | Allowed CORS origins |
| `API_DEBUG` | `true` | Enable debug mode |
| `NEXT_PUBLIC_API_URL` | — | Backend API URL (set to `http://localhost:8000`) |
| `NEXT_PUBLIC_API_KEY` | — | Backend API key (not needed in debug mode) |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, branching strategy, and code quality standards.

## License

See [LICENSE](LICENSE) for details.
