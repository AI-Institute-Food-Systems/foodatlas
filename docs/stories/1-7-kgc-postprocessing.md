# Story 1-7: Postprocessing — Grouping, Common Names, Synonyms

## Goal

Port the postprocessing modules that improve KG quality: entity grouping (food/chemical hierarchies), common name selection, and synonym display generation.

## Depends On

- Story 1-2 (stores, discovery)
- Story 1-6 (initialization — ontology data required by grouping modules)

## Acceptance Criteria

- [ ] `postprocessing/common_name.py` — ported from `update_entity_common_name.py`
- [ ] `postprocessing/synonyms_display.py` — ported from `generate_synonyms_display.py`
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
| `postprocessing/common_name.py` | `FoodAtlas-KGv2/food_atlas/kg/postprocessing/update_entity_common_name.py` |
| `postprocessing/synonyms_display.py` | `FoodAtlas-KGv2/food_atlas/kg/postprocessing/generate_synonyms_display.py` |
| `postprocessing/grouping/` | `FoodAtlas-KGv2/food_atlas/kg/postprocessing/group_entities/` |

## Notes

### common_name.py
- Uses `ID_PREFIX_MAPPER` from `utils/constants.py` (already ported) to skip internal ID-based mentions when counting synonym frequencies.
- Source is a `__main__`-only script. Extract the counting + update logic into functions that accept EntityStore/TripletStore/MetadataStore.

### synonyms_display.py
- For food entities: removes plural synonym forms using `inflection.pluralize`/`singularize`. Add `inflection` to project dependencies.
- For chemical entities: populates display synonyms from ChEBI synonyms and MeSH term names.
- **Depends on MeSH data**: calls `load_mesh()` from the MeSH grouping module. If `mesh.py` is deferred, this chemical-MeSH branch can return an empty list and be filled in later.

### grouping/chemicals.py
- Combines CDNO-based and ChEBI-based chemical grouping.
- **Reuse `_parse_cdno_owl` from `initialization/ontology/cdno.py`** for CDNO hierarchy parsing instead of re-porting the duplicate `load_cdno()` from the source. The grouping module only needs the hierarchy traversal (parent→child DFS) and group assignment logic on top of the parsed data.
- ChEBI grouping reads `chemical_ontology.json` (output of initialization, defined in `stores/schema.py` as `FILE_CHEMICAL_ONTOLOGY`) and traverses the is-a hierarchy to assign groups at a configurable depth level.

### grouping/foods.py
- Reads `food_ontology.json` (output of initialization, defined in `stores/schema.py` as `FILE_FOOD_ONTOLOGY`) and traverses the FoodOn is-a hierarchy.
- Contains a `clean_groups()` heuristic that resolves ambiguous multi-group assignments (e.g., if both "fruit" and "other plant", drop "other plant").
- Uses entity LUT to resolve food group names to entity IDs — requires `EntityStore._lut_food`.

### grouping/mesh.py
- Source is 166 lines and uses `pandarallel` for parallel apply. **Replace `pandarallel` with standard pandas** (the parallelism is not needed for the data sizes involved).
- Depends on pre-processed MeSH data files (`mesh_desc_cleaned.json`, `mesh_supp_cleaned.json`) which come from the data processing step in Story 1-6.
- Also used by `synonyms_display.py` for MeSH synonym lookup — this is the coupling point between the two modules.
- The source `__main__` block has an ad-hoc CSV export (`"for_trevor"`); do not port that. Only port the `load_mesh()` function and the entity→MeSH category mapping logic.
