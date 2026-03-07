# Story 1-10: Cleanup — Delete Legacy, Final Lint Pass

## Goal

Delete the cloned `FoodAtlas-KGv2/` directory, update gitignore, and do a final lint/type-check/coverage pass to ensure everything is clean.

## Depends On

- All previous stories (1-1 through 1-9)

## Acceptance Criteria

- [ ] `FoodAtlas-KGv2/` directory deleted from `backend/kgc/`
- [ ] Old `src/__init__.py` removed (replaced by top-level packages)
- [ ] `pyproject.toml` updated:
  - `packages` in hatch config lists all top-level packages (kg, models, pipeline, query, preprocessing, initialization, postprocessing, integration, data_processing, utils)
  - `--cov` in pytest config covers all packages
- [ ] `.gitignore` updated to exclude:
  - `backend/kgc/outputs/` (pipeline output)
  - `backend/kgc/data/` (downloaded ontologies)
  - `backend/kgc/config/local.json` (local overrides)
- [ ] `ruff check backend/kgc/` — zero errors
- [ ] `ruff format --check backend/kgc/` — zero changes needed
- [ ] `mypy backend/kgc/` — zero errors (no `# type: ignore`)
- [ ] `bandit -r backend/kgc/ --exclude "**/tests"` — zero issues (no `# noqa`)
- [ ] `cd backend/kgc && uv run pytest` — all tests pass, 80%+ coverage
- [ ] No file in `backend/kgc/` exceeds 300 lines
- [ ] CI pipeline (`ci.yml`) correctly detects KGC changes and runs tests

## Verification Against Examples

- [ ] Run the full pipeline against `examples/inputs/` and diff output against `examples/kg/`
  - Entity count, triplet count, metadata count should match
  - Entity IDs, common names, synonyms, external_ids should match
  - Triplet head/tail/relationship linkage should match
  - Lookup tables should match
- [ ] Output format is JSON/JSONL/Parquet (not TSV), but content must be equivalent to the example TSVs

## Notes

- This is the final gating story before the PR is ready.
- Run all checks manually to verify pre-commit hooks will pass.
- Verify `uv sync` still works from a clean state (delete `.venv` and re-sync).
- The `examples/` directory stays in the repo as a regression test fixture.
