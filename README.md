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

- [uv](https://docs.astral.sh/uv/) (Python package manager; handles Python installation automatically)
- Node.js 20+
- npm

### Clone and set up

```bash
git clone https://github.com/AI-Institute-Food-Systems/foodatlas.git
cd foodatlas
./scripts/check-prereqs.sh
```

The setup script auto-installs uv, git hooks, backend dependencies, and frontend dependencies. Node.js and npm must be installed separately.

### Backend

```bash
cd backend/api
uv run pytest
```

### Frontend

```bash
cd frontend
npm run dev     # dev server
npm run build   # production build
npm run lint    # ESLint + type check
npm test        # Vitest
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, branching strategy, and code quality standards.

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
