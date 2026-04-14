#!/usr/bin/env bash
#
# Check and install development prerequisites.
# Auto-installs: uv, git hooks, backend deps, frontend deps.
# Reports only: Node.js, npm (too many competing version managers).
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

errors=0

check_version() {
  local actual="$1" required="$2" name="$3"
  if [ "$(printf '%s\n' "$required" "$actual" | sort -V | head -n1)" != "$required" ]; then
    echo "  OUTDATED: $name $actual (need $required+)"
    errors=$((errors + 1))
    return 1
  fi
  echo "  OK: $name $actual"
  return 0
}

echo "Checking prerequisites..."
echo ""

# uv
echo "[uv]"
if command -v uv &>/dev/null; then
  uv_version="$(uv --version | awk '{print $2}')"
  echo "  OK: uv $uv_version"
else
  echo "  Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  echo "  OK: uv installed"
fi

# Node.js
echo "[Node.js]"
if command -v node &>/dev/null; then
  node_version="$(node --version | sed 's/^v//')"
  check_version "$node_version" "20" "node" || true
else
  echo "  MISSING: node — install from https://nodejs.org/"
  errors=$((errors + 1))
fi

# npm
echo "[npm]"
if command -v npm &>/dev/null; then
  npm_version="$(npm --version)"
  echo "  OK: npm $npm_version"
else
  echo "  MISSING: npm — installed with Node.js"
  errors=$((errors + 1))
fi

echo ""

# Git hooks
echo "[Git hooks]"
if [ -f "$PROJECT_ROOT/.git/hooks/pre-commit" ] && [ -f "$PROJECT_ROOT/.git/hooks/pre-push" ]; then
  echo "  OK: pre-commit and pre-push hooks installed"
else
  echo "  Installing git hooks..."
  "$SCRIPT_DIR/setup-git-hooks.sh"
  echo "  OK: git hooks installed"
fi

# Backend dependencies
echo "[Backend dependencies]"
for project in api db ie kgc; do
  dir="$PROJECT_ROOT/backend/$project"
  if [ -d "$dir/.venv" ]; then
    echo "  OK: backend/$project"
  else
    echo "  Installing backend/$project..."
    (cd "$dir" && uv sync)
    echo "  OK: backend/$project"
  fi
done

# Frontend dependencies
echo "[Frontend dependencies]"
if [ -d "$PROJECT_ROOT/frontend/node_modules" ]; then
  echo "  OK: frontend"
else
  echo "  Installing frontend..."
  (cd "$PROJECT_ROOT/frontend" && npm ci)
  echo "  OK: frontend"
fi

echo ""
if [ "$errors" -eq 0 ]; then
  echo "All prerequisites satisfied."
else
  echo "$errors issue(s) found. See above for details."
  exit 1
fi
