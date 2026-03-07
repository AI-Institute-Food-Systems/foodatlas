# Story 1-3: Query Layer — NCBI and PubChem APIs

## Goal

Port the external API query modules (NCBI Taxonomy, PubChem) with proper configuration injection, caching, and no module-level side effects.

## Depends On

- Story 1-1 (config/settings)

## Acceptance Criteria

- [ ] `query/ncbi.py` — NCBI Taxonomy query functions ported from `_query.py`
  - `Entrez.email` and API key injected via `KGCSettings`, not read from file at import time
  - All query functions accept settings as a parameter
- [ ] `query/pubchem.py` — PubChem query functions ported from `_query.py`
  - Interactive `input()` call removed; replaced with `--pubchem-mapping-file` CLI option
  - Log a clear error if new chemicals found and no mapping file provided
- [ ] `query/cache.py` — shared caching logic extracted
  - Per-100-query incremental save pattern preserved
  - Cache directory configurable via `KGCSettings.cache_dir`
- [ ] No module-level side effects (no file reads, no API initialization at import time)
- [ ] Replace `print()` with `logging`
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines (original `_query.py` is 307 lines)
- [ ] Tests for:
  - Cache hit/miss behavior (using temp directories)
  - Query function signatures accept settings
  - Error handling when API key is missing

## Source Files

| Target | Source |
|--------|--------|
| `query/ncbi.py` | `FoodAtlas-KGv2/food_atlas/kg/_query.py` (NCBI functions) |
| `query/pubchem.py` | `FoodAtlas-KGv2/food_atlas/kg/_query.py` (PubChem functions) |
| `query/cache.py` | Cache patterns extracted from `_query.py` |

## Key Refactors

- `_query.py` lines 24-32: reads `api_key.txt` at module scope — move to `KGCSettings`
- `_query.py` line 241: interactive `input()` for PubChem ID exchange — remove entirely
- Split the file along NCBI vs PubChem boundary (roughly 150 lines each)
