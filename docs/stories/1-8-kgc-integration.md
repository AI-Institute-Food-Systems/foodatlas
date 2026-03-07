# Story 1-8: Integration — FDC, CTD, FlavorDB Merging

## Goal

Port the external database integration modules (FDC, CTD disease correlations, FlavorDB flavors). Split oversized files.

## Depends On

- Story 1-2 (entities, triplets, metadata)
- Story 1-5 (KnowledgeGraph class)

## Acceptance Criteria

- [ ] `integration/fdc.py` — ported from `merge_fdc.py`
  - Replace `global metadata_rows` with return values / list accumulator
- [ ] `integration/ctd/processing.py` — ported from `run_processing_ctd.py`
- [ ] `integration/ctd/pmid_mapping.py` — ported from `make_pmid_to_pmcid.py`
- [ ] `integration/ctd/merger.py` — ported from `merge_ctd.py`
- [ ] `integration/ctd/factd_parser.py` — split from `create_factd_data.py` (530 lines)
- [ ] `integration/ctd/factd_builder.py` — split from `create_factd_data.py`
- [ ] `integration/ctd/utils/data_loaders.py` — split from `data.py` (643 lines)
- [ ] `integration/ctd/utils/data_transforms.py` — split from `data.py`
- [ ] `integration/flavordb/food_flavors.py` — ported from `food_flavors.py`
- [ ] `integration/flavordb/hsdb_loader.py` — ported from `_load_hsdb.py`
- [ ] All paths configurable via `KGCSettings`
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - FDC merge logic (no `global` usage)
  - CTD data loading and transformation
  - FlavorDB flavor triplet creation

## Source Files

| Target | Source |
|--------|--------|
| `integration/fdc.py` | `FoodAtlas-KGv2/food_atlas/kg/merge_dbs/merge_fdc.py` |
| `integration/ctd/` | `FoodAtlas-KGv2/food_atlas/kg/merge_dbs/ctd/` |
| `integration/flavordb/` | `FoodAtlas-KGv2/food_atlas/kg/merge_dbs/flavordb/` |

## Key Splits

- `ctd/utils/data.py` (643 lines) -> `data_loaders.py` (file reading, parsing) + `data_transforms.py` (data manipulation, filtering)
- `ctd/create_factd_data.py` (530 lines) -> `factd_parser.py` (input parsing) + `factd_builder.py` (output construction)
- `merge_fdc.py` line 121: `global metadata_rows` inside `apply()` -> refactor to list accumulator or `iterrows()` pattern
