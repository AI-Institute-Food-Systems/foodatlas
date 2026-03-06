# [Project Title]

[One-paragraph project description. What does this project do? What problem does it solve?]

## 0. Project Setup

After cloning this template, run these steps to configure it for your project:

1. **Rename the source package** — rename `src/mypackage/` to your package name
2. **Update `pyproject.toml`** — set `name`, `description`, and `--cov=src` target to match your package
3. **Update `.pre-commit-config.yaml`** — update any paths that reference the old package name
4. **Set up git hooks**
   ```bash
   ./scripts/setup-git-hooks.sh
   ```
5. **Set up Claude Code** (optional)
   ```bash
   ./scripts/setup-claude-code.sh
   ```
6. **Install dependencies**
   ```bash
   uv sync
   ```

## 1. Directories

```
.
├── src/
│   └── mypackage/      # Rename to your package name
│       └── __init__.py
├── tests/              # Test files
├── data/               # Data files
├── outputs/            # Output files
└── pyproject.toml      # Project configuration
```

## 2. Getting Started

Tested environments:
- Python [version] (e.g., 3.10+)
- Ubuntu [version]
- CUDA [version] (if applicable)

### 2a. Clone this repository

```bash
git clone https://github.com/[username]/[repo].git
cd [repo]
```

### 2b. Install uv (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2c. Install Mutagen (if syncing to a remote machine)

```bash
# macOS
brew install mutagen-io/mutagen/mutagen

# Linux
curl -sS https://webi.sh/mutagen | sh
```

Run this command to update your PATH:

```bash
source ~/.config/envman/PATH.env
```

Copy the example config and edit it for your setup:

```bash
cp mutagen.yml.example mutagen.yml
# Edit mutagen.yml with your remote host and path
mutagen project start
```

### 2d. Install dependencies

```bash
uv sync
```

### 2e. (Optional) Set up pre-commit hooks

```bash
uv run pre-commit install
uv run pre-commit install --hook-type pre-push
```

### 2f. Run the code

[Describe how to run your project]

```bash
uv run python src/[your_script].py
```

## 3. Authors

- [Your Name] - [@github_username](https://github.com/[username])

## 4. Contact

[Your email or preferred contact method]

## 5. Citation

```bibtex
@misc{[project],
  author = {[Your Name]},
  title = {[Project Title]},
  year = {[Year]},
  url = {https://github.com/[username]/[repo]}
}
```

## 6. License

[License type] - see [LICENSE](LICENSE) for details.

## 7. Acknowledgements

- [Acknowledge contributors, funding, or inspirations]
