# Story 1-7: Postprocessing — Grouping, Common Names, Synonyms

## Goal

Port the postprocessing modules that improve KG quality: entity grouping (food/chemical hierarchies), common name selection, and synonym display generation.

## Depends On

- Story 1-2 (stores, discovery)

## Acceptance Criteria

- [ ] `postprocessing/synonyms_display.py` — ported from `generate_synonyms_display.py`
- [ ] `postprocessing/common_name.py` — ported from `update_entity_common_name.py`
- [ ] `postprocessing/grouping/chemicals.py` — ported, combines `group_chemicals.py` + `_chemical_cdno.py` + `_chemical_chebi.py`
- [ ] `postprocessing/grouping/foods.py` — ported, combines `group_foods.py` + `_food_foodon.py`
- [ ] `postprocessing/grouping/mesh.py` — ported from `_chemical_mesh.py`
- [ ] All paths configurable via `KGCSettings`
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - Common name selection (picks most frequent mention)
  - Synonym display generation
  - Grouping logic with sample ontology hierarchy

## Source Files

| Target | Source |
|--------|--------|
| `postprocessing/synonyms_display.py` | `FoodAtlas-KGv2/food_atlas/kg/postprocessing/generate_synonyms_display.py` |
| `postprocessing/common_name.py` | `FoodAtlas-KGv2/food_atlas/kg/postprocessing/update_entity_common_name.py` |
| `postprocessing/grouping/` | `FoodAtlas-KGv2/food_atlas/kg/postprocessing/group_entities/` |
