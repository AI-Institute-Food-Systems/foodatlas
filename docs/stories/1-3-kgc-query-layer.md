# Story 1-3: Query Layer — NCBI and PubChem APIs

## Goal

Implement the external API query modules (NCBI Taxonomy, PubChem) with proper configuration injection, caching, and no module-level side effects. Query stubs already exist in `discovery/query.py` from Story 1-2.

## Depends On

- Story 1-1 (config/settings)
- Story 1-2 (query stubs in `discovery/query.py`, entity creation in `discovery/food.py` and `discovery/chemical.py`)

## Acceptance Criteria

- [ ] `discovery/query.py` — implement `query_ncbi_taxonomy()` and `query_pubchem_compound()` (replace current stubs)
  - `Entrez.email` and API key injected via `KGCSettings`, not read from file at import time
  - All query functions accept settings as a parameter
- [ ] `discovery/cache.py` — shared caching logic extracted
  - Per-100-query incremental save pattern preserved
  - Cache directory configurable via `KGCSettings.cache_dir`
- [ ] PubChem: interactive `input()` call removed; replaced with `--pubchem-mapping-file` CLI option
  - Log a clear error if new chemicals found and no mapping file provided
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
| `discovery/query.py` | `FoodAtlas-KGv2/food_atlas/kg/_query.py` (implement stubs) |
| `discovery/cache.py` | Cache patterns extracted from `_query.py` |

## Key Refactors

- `_query.py` lines 24-32: reads `api_key.txt` at module scope — move to `KGCSettings`
- `_query.py` line 241: interactive `input()` for PubChem ID exchange — remove entirely
- Split the file along NCBI vs PubChem boundary if needed (roughly 150 lines each)

## Notes

- Query stubs in `discovery/query.py` currently raise `NotImplementedError`. This story replaces them with real implementations.
- The `discovery/` module groups query logic with entity creation — queries feed directly into `discovery/food.py` and `discovery/chemical.py`.
