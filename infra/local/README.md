# FoodAtlas Local Infrastructure

Files for running FoodAtlas on a single developer machine. **For the full local-development setup walkthrough (prerequisites, env vars, end-to-end commands), see [`../README.md`](../README.md).** This file is a quick reference for what's in this directory.

## Contents

```
local/
├── README.md           # This file
├── docker-compose.yml  # Local PostgreSQL 16 container definition
├── init-db.sql         # Postgres init script (enables pg_trgm extension)
└── scripts/
    └── run_monthly.sh  # IE → KGC → DB → S3 orchestrator
```

## Common commands

Start the local Postgres container (from the repo root):

```
docker compose -f infra/local/docker-compose.yml up -d
```

Stop and remove it:

```
docker compose -f infra/local/docker-compose.yml down
```

Wipe the local database (deletes the named volume):

```
docker compose -f infra/local/docker-compose.yml down -v
```

Run the full monthly pipeline (IE → KGC → DB load → S3 upload):

```
bash infra/local/scripts/run_monthly.sh
```

Run with stages skipped:

```
bash infra/local/scripts/run_monthly.sh --skip-ie       # Reuse existing IE outputs
bash infra/local/scripts/run_monthly.sh --ie-only       # IE only, stop before KGC
bash infra/local/scripts/run_monthly.sh --skip-s3       # Skip the S3 upload step
```

The pipeline reads API keys from environment variables or a `.env` file at the repo root: `OPENAI_API_KEY`, `NCBI_API_KEY`, `NCBI_EMAIL`. Logs land in `infra/local/scripts/logs/<run-date>/`.

## See also

- [`../README.md`](../README.md) — full operations guide (local + AWS)
- [`../cdk/README.md`](../cdk/README.md) — CDK CLI reference for the AWS production deploy
