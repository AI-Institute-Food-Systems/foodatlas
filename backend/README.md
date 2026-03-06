# FoodAtlas Backend

The backend is organized into four independent Python sub-projects, each with its own dependencies and test suite.

## Sub-projects

| Directory | Description |
|-----------|-------------|
| [`api/`](api/) | API service |
| [`db/`](db/) | Database layer |
| [`ie/`](ie/) | Information extraction |
| [`kgc/`](kgc/) | Knowledge graph construction |

## Getting Started

Each sub-project is a standalone Python package managed by [uv](https://docs.astral.sh/uv/). To work on one:

```bash
cd backend/<project>
uv sync
```

## Running Tests

```bash
cd backend/<project>
uv run pytest
```

## Project Structure

Each sub-project follows the same layout:

```
backend/<project>/
├── pyproject.toml
├── main.py
├── src/<project>/
│   └── __init__.py
└── tests/
    └── test_example.py
```
