# Story 1-2: Core Data Model — Stores, Entity Discovery

## Goal

Port the core KG runtime containers (stores) that wrap pandas DataFrames: EntityStore, TripletStore, MetadataContainsStore. Also port the runtime entity discovery logic that creates new entities from external sources (NCBI Taxonomy, PubChem).

## Depends On

- Story 1-1 (models, utils, config)

## Acceptance Criteria

- [x] `stores/schema.py` — column definitions derived from Pydantic models via `model_fields` + aliases
  - `ENTITY_COLUMNS`, `TRIPLET_COLUMNS`, `METADATA_CONTAINS_COLUMNS` — single source of truth
  - `TSV_SEP`, `INDEX_COL` — format constants
  - `FILE_ENTITIES`, `FILE_TRIPLETS`, `FILE_METADATA_CONTAINS`, `FILE_LUT_FOOD`, `FILE_LUT_CHEMICAL` — file name constants
- [x] `stores/entity_store.py` — EntityStore ported from `_base.py`
  - Dict-based dispatch for LUT selection (replaces `eval()` in original)
  - Lookup tables stored as JSON (not TSV)
  - `_load_lut()` / `_save_lut()` helpers for JSON I/O
  - Type annotations on all methods, `Path` objects for file paths
- [x] `stores/triplet_store.py` — TripletStore ported from `_triplets.py`
  - `_make_key()` static method for composite key construction
  - `_insert_or_merge()` and `_resolve_all_metadata()` extracted from `create()`
  - `_key_to_metadata` dict for deduplication lookup
- [x] `stores/metadata_store.py` — MetadataContainsStore ported from `_metadata.py`
  - `_nan_to_empty()` helper for TSV loading
  - Internal DataFrame stored as `_records`
- [x] `discovery/__init__.py`, `discovery/food.py`, `discovery/chemical.py` — runtime entity creation
  - Uses `FoodEntity` / `ChemicalEntity` model constructors + `model_dump(by_alias=True)` for DataFrame rows
  - Column lists imported from `stores/schema.py` (no hardcoded COLUMNS)
- [x] `discovery/query.py` — query stubs for NCBI Taxonomy & PubChem (implemented in Story 1-3)
- [x] Replace `print()` with `logging` in all ported files
- [x] All files pass `ruff check` and `mypy`
- [x] All files under 300 lines
- [x] Tests for:
  - Entity creation (food + chemical), ID generation (e0, e1, ...)
  - Lookup table (LUT) operations: add, query, disambiguation
  - Triplet creation, deduplication, metadata_ids merging
  - Metadata column validation
  - Round-trip: create -> save -> reload -> verify equality
  - Shared test fixtures in `tests/conftest.py`

## Source Files

| Target | Source |
|--------|--------|
| `stores/entity_store.py` | `FoodAtlas-KGv2/food_atlas/kg/entities/_base.py` |
| `stores/triplet_store.py` | `FoodAtlas-KGv2/food_atlas/kg/_triplets.py` |
| `stores/metadata_store.py` | `FoodAtlas-KGv2/food_atlas/kg/_metadata.py` |
| `stores/schema.py` | New — derives columns from Pydantic models |
| `discovery/food.py` | `FoodAtlas-KGv2/food_atlas/kg/entities/_food.py` |
| `discovery/chemical.py` | `FoodAtlas-KGv2/food_atlas/kg/entities/_chemical.py` |
| `discovery/query.py` | Stubs for `FoodAtlas-KGv2/food_atlas/kg/_query.py` |

## Key Design Decisions

- **Models as schema source of truth**: Column lists are derived from Pydantic model fields via `stores/schema.py`, eliminating duplicated `COLUMNS` lists across files.
- **Aliases bridge model ↔ TSV names**: `Entity.synonyms_display` serializes to `_synonyms_display` in TSV via `Field(alias=...)`. Entity creation uses `model.model_dump(by_alias=True)` to produce TSV-compatible dicts.
- **LUT format changed from TSV to JSON**: Lookup tables are `dict[str, list[str]]` — JSON is the natural format, eliminating `literal_eval` parsing and DataFrame round-tripping.
- **`src/entities/` renamed to `src/discovery/`**: Groups runtime entity discovery (NCBI/PubChem queries + entity creation) together with the query stubs. Distinct from the initialization pipeline (Story 1-6) which seeds the KG from ontologies (FoodOn, ChEBI).
- **No BaseStore ABC**: Stores differ too much internally (EntityStore has LUTs, TripletStore has dedup, MetadataContainsStore has auto-IDs) for inheritance to help.
