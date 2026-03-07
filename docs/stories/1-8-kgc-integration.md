# Story 1-8: Integration — FDC, CTD, FlavorDB Merging

## Goal

Port the external database integration modules (FDC, CTD disease correlations, FlavorDB flavors). Split oversized files.

## Depends On

- Story 1-2 (stores, discovery)
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

## Implementation Notes

### Current state

The `integration/` directory does not exist yet. All 11 target files and their tests need to be created from scratch. `KGCSettings` already has an `integration_dir` field (`src/models/settings.py:27`).

### FDC (`merge_fdc.py`, 185 lines)

- `_get_metadatum()` uses `global metadata_rows` (line 121) — refactor to list accumulator passed via closure or `iterrows()` pattern
- Hardcoded `PATH_FDC_DATA_DIR` — wire through `KGCSettings`
- `__main__` block (lines 101-185) has orchestration logic that needs to become callable functions (load mappers, create metadata, build triplets, merge into KG)

### CTD data utilities (`ctd/utils/data.py`, 643 lines → two files)

Split by responsibility:

**`data_loaders.py`** — file I/O and parsing:
- `load_ctd_data`, `load_foodatlas_data`, `load_pubchem_to_ctd`, `load_pubchem_to_ctd_mapping`, `load_pmid_to_pmcid_mapping`, `load_pubchem_to_cas`, `load_pubchem_to_cas_mapping`, `load_oft_data`, `load_tvd_data`
- All filename constants (`CTD_CHEMDIS_DATA_FILENAME`, etc.)
- All column name constants (`FA_ID`, `CTD_CHEMICAL_ID`, etc.)

**`data_transforms.py`** — data manipulation and filtering:
- `change_content_to_list`, `split_column_into_length`, `adjust_FA_entities_data`, `adjust_FA_chemicals_data`
- `create_pmid_to_pmcid_mapping` (HTTP + parsing logic)
- Mapping constants (`CTD_ALTID_MAPPING`, `CTD_DIRECTEVIDENCE_MAPPING`, `OFT_COLUMN_MAPPING`, etc.)

Cross-cutting concerns:
- Replace all `os.path` with `pathlib.Path` (ruff PTH rules)
- Remove `openpyxl` escape import if not needed in the split target
- `type` used as parameter name shadows builtin — rename to `data_type` or `kind`

### CTD factd (`create_factd_data.py`, 530 lines → two files)

**`factd_parser.py`** — input parsing and entity creation (~250 lines):
- `add_mesh_ids_to_fa_chemicals`, `get_max_fa_id`, `filter_ctd_chemdis`
- `get_disease_ids_from_alt_disease_ids`, `create_disease_entities`
- `create_mapping_ctd_to_fa`

**`factd_builder.py`** — triplet/metadata construction + orchestration (~250 lines):
- `create_disease_triplets_metadata` (the full version with `fa_chem_lookup` param)
- `main()` orchestration (currently a `click` CLI command)
- Remove `click` decorator, make paths configurable via `KGCSettings`

### Duplicate `create_disease_triplets_metadata`

Two versions exist — use the `merge_ctd.py:19` version as the canonical one:
- Cleaner inline ID mapping (direct `external_ids` iteration, no `adjust_FA_entities_data` indirection)
- Correct 1:N cardinality via explode (handles one MeSH ID mapping to multiple FA entities)
- Self-contained — no dependency on `create_mapping_ctd_to_fa` or `fa_chem_lookup`
- Graceful on missing IDs (returns `[]` instead of `KeyError`)

From `create_factd_data.py`, only port the standalone helpers that `merge_ctd.py` already imports:
- `create_disease_entities` → `factd_parser.py`
- `get_max_fa_id` → `factd_parser.py`

Drop from the full version: `create_mapping_ctd_to_fa`, `add_mesh_ids_to_fa_chemicals`, `filter_ctd_chemdis`, `head_id_name` fallback, mismatch file export. These add complexity for marginal coverage and are not used by the simplified pipeline.

### CTD processing (`run_processing_ctd.py`, 31 lines)

Straightforward port. Remove `__main__` guard, make a callable function. Wire `data_dir` and `output_dir` through `KGCSettings`.

### CTD PMID mapping (`make_pmid_to_pmcid.py`, 49 lines)

- Remove `click` CLI decorator, make it a callable function
- Replace `os.path` with `pathlib.Path`
- Wire paths through `KGCSettings`

### CTD merger (`merge_ctd.py`, 248 lines)

- Large amount of commented-out code (lines 76-127) — remove entirely
- Has its own simplified `create_disease_triplets_metadata` — reconcile with `factd_builder.py`
- `__main__` block (lines 196-248) with hardcoded paths — make callable

### FlavorDB food flavors (`food_flavors.py`, 292 lines)

- Uses `torch.load()` for loading scraped FlavorDB data — unusual dependency, review if pickle/JSON alternative is viable
- Uses `fuzzywuzzy` (deprecated) — consider replacing with `thefuzz`
- Uses `pandarallel` for parallel apply — keep but make optional
- Already under 300 lines, fits as single file
- `generate_and_merge_flavor_data()` is the main entry point — refactor to accept `KGCSettings`

### FlavorDB HSDB loader (`_load_hsdb.py`, 45 lines)

- Hardcoded `"data/HSDB/"` path — wire through `KGCSettings`
- Uses bare `open()` without context manager — fix
- Small file, straightforward port

### Dependencies to verify in `pyproject.toml`

Source files import: `pandas`, `pandarallel`, `tqdm`, `fuzzywuzzy`/`thefuzz`, `torch`, `requests`, `beautifulsoup4`, `openpyxl`, `click`. Check which are already present and add missing ones.

### `ctd/utils/logging.py` (47 lines)

Provides a `get_logger()` helper. Decide whether to port it or replace with standard `logging.getLogger()` usage throughout.
