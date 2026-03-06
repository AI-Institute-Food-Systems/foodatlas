# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TODO: Add a brief description of the project, its purpose, and main features.

## Commands

TODO: Add any custom commands or scripts that are relevant to the project.

```bash
# Python scripts should always be run with the -m flag. Look into pyproject.toml for any specific environment management tools like Poetry or virtualenv:
python -m src.main
poetry run python -m src.main
uv run python -m src.main
```

## Architecture

TODO: Describe the overall architecture of the project, including key components and their interactions.

```
src/
├── module_1/
│   ├── submodule_1/
│   │   ├── script_1.py     # Short description
│   │   └── script_2.py     # Short description
│   └── submodule_2/
│       └── ...      # System prompt for restaurant assistant persona
├── module_2/
│   └── ...         # Another module description
└── main.py             # Entry point of the application
```

## API Endpoints

TODO: List and describe the main API endpoints if applicable.

## Key Patterns

TODO: Highlight any important design patterns, coding conventions, or architectural decisions.

## Code Standards

Configured in `pyproject.toml`:
- **Python**: 3.12+ required
- **Ruff**: E, W, F, I, B, C4, UP, ARG, SIM, TCH, PTH, ERA, PL, RUF rules
- **MyPy**: Strict mode with type checking
- **Bandit**: Security scanning excluding tests

## General Rules

1. **File size limit**: Do not allow code files to exceed 300 lines. Refactor by splitting into smaller modules.
2. **No lazy bypasses**: Do not use `# noqa`, `# type: ignore` to bypass errors. Fix the underlying issue.
3. **Rely on pre-commit hooks**: Pre-commit hooks run on commit (ruff, mypy, bandit) and push (pytest). Only run checks manually when debugging.
4. **No cheating on test coverage**: Do not lower `--cov-fail-under` threshold or add files to `[tool.coverage.run] omit` to bypass failing coverage. Write proper tests instead.
