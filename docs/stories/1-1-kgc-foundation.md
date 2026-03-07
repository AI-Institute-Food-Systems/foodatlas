# Story 1-1: Foundation — Config, Models, Utils

## Goal

Set up the project skeleton: `pyproject.toml` with real dependencies, `config/`, `models/`, and `utils/`. This is the base that all other stories depend on. No behavioral code yet.

## Acceptance Criteria

- [x] `pyproject.toml` updated with all dependencies (pandas, click, biopython, inflection, tqdm, pympler, pydantic-settings, owlready2, rdflib, lxml, openpyxl, numpy)
- [x] `uv sync` succeeds
- [x] `config/defaults.json` created with default paths and empty API key placeholders
- [x] `models/__init__.py` exports all models
- [x] `models/settings.py` — `KGCSettings` with Pydantic Settings, env prefix `KGC_`, loads defaults from `config/defaults.json`
- [x] `models/entity.py` — Entity, FoodEntity, ChemicalEntity pydantic models matching TSV schema (foodatlas_id, entity_type, common_name, scientific_name, synonyms, external_ids, synonyms_display)
  - Uses `ConfigDict(populate_by_name=True)` and `Field(alias="_synonyms_display")` so `model_dump(by_alias=True)` produces TSV-compatible column names
- [x] `models/triplet.py` — Triplet model (foodatlas_id, head_id, relationship_id, tail_id, metadata_ids)
- [x] `models/metadata.py` — MetadataContains model (foodatlas_id, conc_value, conc_unit, food_part, food_processing, source, reference, entity_linking_method, quality_score, food_name_raw, chemical_name_raw, conc_raw, food_part_raw)
  - Uses `ConfigDict(populate_by_name=True)` and `Field(alias=...)` for `_food_name`, `_chemical_name`, `_conc`, `_food_part`
- [x] `models/relationship.py` — Relationship model (r1-r5)
- [x] `models/version.py` — KGVersion model
- [x] `utils/__init__.py`, `utils/constants.py`, `utils/merge_sets.py` ported from `FoodAtlas-KGv2/food_atlas/kg/utils/`
- [x] `preprocessing/constants/` ported (greek_letters.py, punctuations.py, units.py)
- [x] All files pass `ruff check` and `mypy`
- [x] All files under 300 lines
- [x] Tests for settings loading (env vars, defaults.json fallback), model validation, and alias serialization/validation

## Source Files

| Target | Source |
|--------|--------|
| `models/settings.py` | New (replaces hardcoded paths in all original files) |
| `models/entity.py` | Schema from `FoodAtlas-KGv2/food_atlas/kg/entities/_base.py` |
| `models/triplet.py` | Schema from `FoodAtlas-KGv2/food_atlas/kg/_triplets.py` |
| `models/metadata.py` | Schema from `FoodAtlas-KGv2/food_atlas/kg/_metadata.py` |
| `utils/constants.py` | `FoodAtlas-KGv2/food_atlas/kg/utils/constants.py` |
| `utils/merge_sets.py` | `FoodAtlas-KGv2/food_atlas/kg/utils/_merge_sets.py` |
| `preprocessing/constants/` | `FoodAtlas-KGv2/food_atlas/kg/preprocessing/constants/` |

## I/O Formats

**Input** (from IE sub-project): JSON preferred, pkl supported for backward compatibility. Dict keyed by index, each record has:
```json
{
  "0": {
    "text": "In apricot fruits, β-carotene participates...",
    "pmcid": 7794732,
    "response": "(apricot, fruits, anthocyanins, )\n(apricot, fruits, β-carotene, )",
    "triplets": [["apricot", "fruits", "anthocyanins", ""], ["apricot", "fruits", "β-carotene", ""]]
  }
}
```
Triplet tuple: `[food_name, food_part, chemical_name, concentration]`

**Output**: JSON/JSONL/Parquet (configurable via `KGCSettings.output_format`). No more TSV.

## Example Data

Reference examples at `backend/kgc/examples/` for verification:
- `examples/inputs/` — sample IE extraction outputs (JSON)
- `examples/kg/` — expected pipeline outputs (entities, triplets, metadata, lookup tables, etc.)
- Use these to verify the rewritten pipeline produces equivalent results

## Notes

- Pydantic models define the **data contract** — they don't replace the pandas DataFrame operations in the stores. The store classes use DataFrames internally but validate I/O through these models.
- Models use `Field(alias=...)` with `populate_by_name=True` to bridge the gap between Python field names (e.g., `synonyms_display`) and TSV column names (e.g., `_synonyms_display`). `model_dump(by_alias=True)` produces TSV-compatible dicts.
- `KGCSettings` should support both env vars (`KGC_KG_DIR`) and `config/defaults.json` as fallback.
- `KGCSettings.output_format` should default to `"jsonl"` with options `"json"`, `"jsonl"`, `"parquet"`.
