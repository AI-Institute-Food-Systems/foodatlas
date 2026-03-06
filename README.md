# FoodAtlas

A food knowledge graph platform. This monorepo contains the frontend, backend services, and infrastructure code.

## Repository Structure

```
.
├── frontend/               # Next.js web app (deployed on Vercel)
├── backend/
│   ├── api/                # API service
│   ├── db/                 # Database layer
│   ├── ie/                 # Information extraction
│   └── kgc/                # Knowledge graph construction
├── infra/                  # AWS CDK infrastructure (Python)
├── .github/workflows/      # CI/CD pipelines
├── pyproject.toml          # Shared linter/checker configs (ruff, mypy, bandit)
└── .pre-commit-config.yaml # Git hooks
```

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (Python package manager)

### Clone and set up

```bash
git clone https://github.com/AI-Institute-Food-Systems/foodatlas.git
cd foodatlas
```

### Install uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Set up git hooks

```bash
./scripts/setup-git-hooks.sh
```

### Install dependencies for a backend sub-project

Each backend sub-project is independent. Navigate to it and install:

```bash
cd backend/api
uv sync
```

### Run tests

```bash
cd backend/api
uv run pytest
```

## Authors

- [Your Name] - [@github_username](https://github.com/[username])

## Contact

[Your email or preferred contact method]

## Citation

```bibtex
@misc{foodatlas,
  author = {[Your Name]},
  title = {FoodAtlas},
  year = {2026},
  url = {https://github.com/AI-Institute-Food-Systems/foodatlas}
}
```

## License

See [LICENSE](LICENSE) for details.

## Acknowledgements

- [Acknowledge contributors, funding, or inspirations]
