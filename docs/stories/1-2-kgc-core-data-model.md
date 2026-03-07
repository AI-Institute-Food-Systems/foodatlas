# Story 1-2: Core Data Model — Entities, Triplets, Metadata

## Goal

Port the core KG data classes that wrap pandas DataFrames: Entities, Triplets, Metadata. These are the runtime representations used throughout the pipeline.

## Depends On

- Story 1-1 (models, utils, config)

## Acceptance Criteria

- [ ] `kg/entities/__init__.py`, `kg/entities/base.py` — Entities class ported from `_base.py`
  - Replace `eval(f"self.entities._lut_{entity_type}")` with dict-based dispatch
  - Add type annotations to all methods
  - Accept `Path` objects instead of string paths
  - Accept `KGCSettings` for path configuration
- [ ] `kg/entities/food.py` — food entity creation ported from `_food.py`
- [ ] `kg/entities/chemical.py` — chemical entity creation ported from `_chemical.py`
- [ ] `kg/triplets.py` — Triplets class ported from `_triplets.py`
- [ ] `kg/metadata.py` — Metadata class ported from `_metadata.py`
- [ ] Replace `print()` with `logging` in all ported files
- [ ] Replace `pandarallel.initialize()` at module scope with optional try/except fallback
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - Entity creation (food + chemical), ID generation (e0, e1, ...)
  - Lookup table (LUT) operations: add, query, disambiguation
  - Triplet creation, deduplication, metadata_ids merging
  - Metadata column validation
  - Round-trip: create -> save TSV -> reload -> verify equality

## Source Files

| Target | Source |
|--------|--------|
| `kg/entities/base.py` | `FoodAtlas-KGv2/food_atlas/kg/entities/_base.py` |
| `kg/entities/food.py` | `FoodAtlas-KGv2/food_atlas/kg/entities/_food.py` |
| `kg/entities/chemical.py` | `FoodAtlas-KGv2/food_atlas/kg/entities/_chemical.py` |
| `kg/triplets.py` | `FoodAtlas-KGv2/food_atlas/kg/_triplets.py` |
| `kg/metadata.py` | `FoodAtlas-KGv2/food_atlas/kg/_metadata.py` |

## Key Refactors

- `_base.py` uses `eval()` for LUT selection — replace with `{"food": self._lut_food, "chemical": self._lut_chemical}[entity_type]`
- Column constants (`COLUMNS`) should reference or align with pydantic models from Story 1-1
- TSV I/O methods should accept `Path` from settings, not hardcoded strings
