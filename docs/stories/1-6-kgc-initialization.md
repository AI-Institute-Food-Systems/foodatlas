# Story 1-6: Initialization — Seed KG from Ontologies

## Goal

Port the KG initialization modules that create empty files and seed entities from ontologies (FoodOn, ChEBI, CDNO, MeSH, PubChem, FDC).

## Depends On

- Story 1-2 (stores, discovery)
- Story 1-3 (query layer for NCBI/PubChem lookups)

## Acceptance Criteria

- [ ] `initialization/scaffold.py` — ported from `create_empty_files.py`
  - Use `KGCSettings.kg_dir` instead of hardcoded `"outputs/kg"`
  - Use column constants from `stores/schema.py` (`ENTITY_COLUMNS`, `TRIPLET_COLUMNS`, etc.)
- [ ] `initialization/food/init_entities.py` — ported
- [ ] `initialization/food/init_onto.py` — ported
- [ ] `initialization/food/loaders.py` — combined `_load_fdc.py` + `_load_foodon.py`
- [ ] `initialization/chemical/init_entities.py` — ported, split if >300 lines
  - Original is 389 lines — must be split
- [ ] `initialization/chemical/init_onto.py` — ported
- [ ] `initialization/chemical/loaders.py` — combined `_load_cdno/chebi/fdc/mesh/pubchem`
- [ ] `data_processing/` — ported (cdno.py, chebi.py, foodon.py, mesh.py, pubchem.py)
  - These preprocess raw ontology data before initialization uses it
- [ ] All paths configurable via `KGCSettings`
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - Scaffold creates correct empty TSV files with expected columns
  - Entity initialization from sample ontology data (small fixtures)

## Source Files

| Target | Source |
|--------|--------|
| `initialization/scaffold.py` | `FoodAtlas-KGv2/food_atlas/kg/initialization/create_empty_files.py` |
| `initialization/food/` | `FoodAtlas-KGv2/food_atlas/kg/initialization/food/` |
| `initialization/chemical/` | `FoodAtlas-KGv2/food_atlas/kg/initialization/chemical/` |
| `data_processing/` | `FoodAtlas-KGv2/food_atlas/data_processing/` |

## Notes

- `chemical/init_entities.py` (389 lines) must be split. Natural boundary: separate loaders per ontology source (ChEBI, CDNO, MeSH, PubChem) into `loaders.py`, keep orchestration in `init_entities.py`.
- Data processing scripts (`run_processing_*.py`) transform raw downloaded ontology files into intermediate formats used by initialization. They depend on `KGCSettings.data_dir`.
