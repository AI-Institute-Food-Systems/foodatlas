# FoodAtlas API

FastAPI REST service for the FoodAtlas knowledge graph.

## Getting Started

Requires a running PostgreSQL instance (see root README for Docker setup).

```bash
uv sync
uv run python main.py
```

The server starts at `http://localhost:8000` with auto-reload enabled.

## Configuration

Settings are read from `API_*` and `DB_*` environment variables (or a `.env` file):

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | (empty) | Bearer token for authentication |
| `API_CORS_ORIGINS` | `http://localhost:3000` | Comma-separated allowed origins |
| `API_DEBUG` | `true` | Skip API key verification when true |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `foodatlas` | Database name |
| `DB_USER` | `foodatlas` | Database user |
| `DB_PASSWORD` | `foodatlas` | Database password |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/food/metadata` | Food entity metadata |
| GET | `/food/profile` | Macro and micro nutrient profile |
| GET | `/food/composition` | Food-chemical composition (paginated) |
| GET | `/chemical/metadata` | Chemical entity metadata |
| GET | `/chemical/composition` | Foods containing a chemical |
| GET | `/disease/metadata` | Disease entity metadata |
| GET | `/disease/correlation` | Chemical-disease correlations (paginated) |
| GET | `/metadata/statistics` | Database statistics |
| GET | `/metadata/search` | Autocomplete search |
| GET | `/download` | Download entries |

## Project Structure

```
api/
├── main.py                 # Uvicorn entry point
├── pyproject.toml
├── src/
│   ├── app.py              # FastAPI application factory
│   ├── config.py           # APISettings (pydantic-settings)
│   ├── dependencies.py     # DB session and auth dependencies
│   ├── routes/             # Route handlers
│   │   ├── food.py
│   │   ├── chemical.py
│   │   ├── disease.py
│   │   ├── metadata.py
│   │   └── download.py
│   ├── repositories/       # Database query logic
│   │   ├── food.py
│   │   ├── chemical.py
│   │   ├── disease.py
│   │   └── search.py
│   └── schemas/            # Pydantic response models
└── tests/
```

## Running Tests

```bash
uv run pytest
```
