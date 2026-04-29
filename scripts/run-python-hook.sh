#!/usr/bin/env bash
# Run a uv command in each Python project that has staged changes.
# Usage: scripts/run-python-hook.sh <command...>
# Example: scripts/run-python-hook.sh ruff check .
#          scripts/run-python-hook.sh bandit -c {ROOT}/pyproject.toml -r {SRC}
#
# Substitutions applied to forwarded arguments:
#   {ROOT} → absolute repo root (so callers don't hard-code "../../").
#   {SRC}  → the project's source directory (src/ for most, stacks/ for CDK).
set -euo pipefail

# Project path → source-directory name. Both fields drive the loop below.
PYTHON_PROJECTS=(
  "backend/api:src"
  "backend/db:src"
  "backend/ie:src"
  "backend/kgc:src"
  "infra/aws:stacks"
)

STAGED=$(git diff --cached --name-only)
REPO_ROOT=$(git rev-parse --show-toplevel)

for entry in "${PYTHON_PROJECTS[@]}"; do
  dir="${entry%:*}"
  src="${entry#*:}"
  if echo "$STAGED" | grep -q "^$dir/"; then
    args=()
    for a in "$@"; do
      a="${a//\{ROOT\}/$REPO_ROOT}"
      a="${a//\{SRC\}/$src}"
      args+=("$a")
    done
    echo "── $dir: uv run ${args[*]}"
    (cd "$dir" && uv run "${args[@]}")
  fi
done
