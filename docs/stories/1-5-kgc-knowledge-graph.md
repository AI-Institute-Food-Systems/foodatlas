# Story 1-5: KG Orchestrator — KnowledgeGraph Class

## Goal

Port the main `KnowledgeGraph` class that orchestrates entity creation, triplet expansion, and synonym disambiguation. This is the central class that ties together entities, triplets, metadata, and queries.

## Depends On

- Story 1-2 (stores, discovery)
- Story 1-3 (query layer)
- Story 1-4 (preprocessing)

## Acceptance Criteria

- [ ] `kg/knowledge_graph.py` — KnowledgeGraph class ported from `_kg.py`
  - Accept `KGCSettings` in constructor (replaces hardcoded `"outputs/kg"` default)
  - Replace `eval()` LUT access with dict dispatch (via Entities.get_lut)
  - Replace `nonlocal self` pattern with explicit parameter passing
  - Replace `print()` with `logging`
- [ ] `kg/disambiguation.py` — extracted if `knowledge_graph.py` exceeds 300 lines
  - Synonym disambiguation logic (`_disambiguate_synonyms` and related methods)
  - Placeholder entity creation/resolution
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - KG load from fixture TSVs
  - Entity creation flow (food + chemical)
  - Triplet creation with deduplication
  - Synonym disambiguation (placeholder entity creation/resolution)
  - Memory stats reporting

## Source Files

| Target | Source |
|--------|--------|
| `kg/knowledge_graph.py` | `FoodAtlas-KGv2/food_atlas/kg/_kg.py` |
| `kg/disambiguation.py` | Extracted from `_kg.py` if needed for 300-line limit |

## Key Refactors

- `_kg.py` line 148: `eval(f"self.entities._lut_{entity_type}")` — use dict dispatch
- `_kg.py` line 228: `nonlocal self` — pass as explicit parameter
- Constructor loads from `self.path_kg` (hardcoded default) — use `KGCSettings.kg_dir`
