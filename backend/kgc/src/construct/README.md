# Construct (Phase 2)

Phase 2 of the KGC pipeline. Reads the standardized parquet files from Phase 1 (ingest) and builds the knowledge graph: filtering, entity resolution, triplet creation, and postprocessing.

## Running

```bash
# Run construct (all stages in order)
uv run python main.py run --stage corrections
```

Construct requires ingest output to exist at `outputs/ingest/`. Run `--stage ingest` first if it doesn't.

## Workflow

Construct runs 5 stages in strict order. Each stage depends on the previous one.

```
outputs/ingest/*.parquet
    │
    ▼
┌─────────────────────────────────────────────────┐
│ 1. CORRECTIONS                                  │
│    Load all 8 sources from ingest parquet.      │
│    Apply corrections.yaml in-memory.            │
│    No files written — DataFrames mutated.       │
├─────────────────────────────────────────────────┤
│ 2. SUBTREE FILTER                               │
│    DFS to find descendants of ontology roots.   │
│    Filter each source to relevant subset.       │
│    No files written — DataFrames filtered.      │
├─────────────────────────────────────────────────┤
│ 3. ENTITY RESOLUTION                            │
│    Three-pass entity creation + linking.        │
│    Writes: outputs/kg/entities.json,            │
│            outputs/kg/lookup_table_*.json       │
├─────────────────────────────────────────────────┤
│ 4. TRIPLET BUILD                                │
│    Create ontology + relationship triplets.     │
│    Writes: outputs/kg/triplets.json,            │
│            outputs/kg/*_ontology.json,          │
│            outputs/kg/metadata_contains.json    │
├─────────────────────────────────────────────────┤
│ 5. POSTPROCESSING                               │
│    Common names, synonym display, grouping.     │
│    Updates: outputs/kg/entities.json            │
└─────────────────────────────────────────────────┘
    │
    ▼
outputs/kg/ (final knowledge graph)
```

### Stage 1: Corrections

**File**: `corrections_applier.py`
**Input**: All 8 sources loaded from `outputs/ingest/` as in-memory DataFrames.
**Config**: `src/config/corrections.yaml`

Applies centralized manual corrections to the ingested data:
- **ChEBI**: Drop invalid nodes (e.g., 194466). Rename misnamed compounds (e.g., 221398 → "15G256nu").
- **CDNO**: Remap deprecated cross-references (e.g., CHEBI_80096 → CHEBI_166888).
- **FDC**: Drop duplicate nutrient IDs (2048, 1008, 1062). Rename nutrients (2047 → "energy").

No files are written. DataFrames are mutated in-place and passed to the next stage.

### Stage 2: Subtree Filter

**File**: `subtree_filter.py`
**Input**: Corrected DataFrames from Stage 1.
**Config**: `ontology_roots` section of `corrections.yaml`.

Uses DFS on the `is_a` edges to compute ontology subtrees, then filters each source:

| Source | Filter | Before → After |
|--------|--------|----------------|
| FoodOn | Descendants of `FOODON_00002381` (food product) + `OBI_0100026` (organism) | 32K → 18K nodes |
| ChEBI | Descendants of `23367` (molecular entity) | 205K → 193K nodes |
| CTD | Edges with non-empty `direct_evidence` only | 9M → 129K edges |
| CDNO | Nodes that have FDC nutrient cross-references | 2K → 261 nodes |

The root IDs come from `corrections.yaml`, not hardcoded in the filter functions.

### Stage 3: Entity Resolution

**Files**: `entity_resolver.py`, `resolve_primary.py`, `resolve_secondary.py`
**Input**: Filtered DataFrames from Stage 2.
**Output**: `outputs/kg/entities.json`, `outputs/kg/lookup_table_food.json`, `outputs/kg/lookup_table_chemical.json`

Three-pass process that creates FoodAtlas entities (`e1`, `e2`, ...) from the filtered source nodes:

**Pass 1 — Primary entities** (independent sources):
- Foods from FoodOn `is_food` nodes → `entity_type = "food"`
- Chemicals from ChEBI `is_molecular_entity` nodes → `entity_type = "chemical"`
- Diseases from CTD disease nodes → `entity_type = "disease"`

Each entity gets a `foodatlas_id`, and its synonyms are registered in the EntityLUT for later lookup.

**Pass 2 — Link secondary sources** via cross-references:
- CDNO → ChEBI: Match `cdno_xrefs` where `target_source = "chebi"`. Add CDNO ID to the matched ChEBI entity's `external_ids`.
- FDC foods → FoodOn: Match `fdc_xrefs` where `target_source = "foodon"`. Add FDC ID to the matched FoodOn entity's `external_ids`.
- FDC nutrients → CDNO/ChEBI: Match via CDNO's `fdc_nutrient` xrefs. Add FDC nutrient ID to the matched entity.

Pass 2 uses pre-built hash maps (O(1) lookup per xref) instead of linear scans.

**Pass 3 — Create unlinked entities**:
- CDNO entries not linked in Pass 2 → new chemical entities
- FDC foods not linked to FoodOn → new food entities
- FDC nutrients not linked via CDNO → new chemical entities

### Stage 4: Triplet Build

**Files**: `triplet_builder.py`, `triplets/food_ontology.py`, `triplets/chemical_ontology.py`, `triplets/food_chemical.py`, `triplets/flavor.py`
**Input**: Entity store from Stage 3 + filtered edges from Stage 2.
**Output**: `outputs/kg/triplets.json`, `outputs/kg/food_ontology.json`, `outputs/kg/chemical_ontology.json`, `outputs/kg/metadata_contains.json`

Creates triplets from the resolved entities and Phase 1 edges:

1. **Food ontology** (`food_ontology.py`): `is_a` triplets from FoodOn edges where both head and tail map to existing food entities.
2. **Chemical ontology** (`chemical_ontology.py`): `is_a` triplets from ChEBI edges where both head and tail map to existing chemical entities.
3. **Food-chemical contains** (`food_chemical.py`): Reads FDC `contains` edges. For each food→nutrient pair, creates metadata (concentration amount, unit) and a CONTAINS triplet. Uses the old `KnowledgeGraph.add_triplets_from_metadata` path which also discovers new entities from FDC names not yet in the entity store.
4. **Flavor descriptions** (`flavor.py`): Maps FlavorDB PubChem CIDs to chemical entities and stores flavor descriptors on the entity.

### Stage 5: Postprocessing

**Delegated to**: `src/postprocessing/` (old code, shared with the legacy pipeline)
**Input**: KnowledgeGraph loaded from Stage 4 output.
**Output**: Updated `outputs/kg/entities.json`.

- **Food grouping**: Traverse FoodOn ontology to assign group labels (dairy, fruit, meat, etc.).
- **Chemical grouping (CDNO)**: Nutrient classification (amino acid, vitamin, mineral, etc.).
- **Chemical grouping (ChEBI)**: Chemical hierarchy classification.
- **Common names**: Select the most-mentioned synonym as `common_name`.
- **Synonym display**: Build `_synonyms_display` dict for UI presentation.

## How construct differs from constructor

There are two directories with similar names:

```
src/construct/     ← Phase 2 pipeline (NEW — this directory)
src/constructor/   ← KnowledgeGraph class + disambiguation (OLD — legacy)
```

**`src/constructor/`** (old, kept for now):
- `knowledge_graph.py`: The `KnowledgeGraph` class — a runtime container that loads entities, triplets, and metadata from JSON, provides `add_triplets_from_metadata()` and `get_triplets()`, and saves back.
- `disambiguation.py`: The placeholder entity system — creates fake entities when a synonym maps to multiple real entities.

**`src/construct/`** (new):
- The Phase 2 pipeline that reads ingest parquet, applies corrections, filters, resolves entities, and builds triplets.
- Does NOT replace `constructor/`. Instead, it **uses** `KnowledgeGraph` from `constructor/` during Stage 4 (triplet build) to write triplets and metadata via the existing `add_triplets_from_metadata` method.

The relationship:
```
construct/runner.py
    ├── Stage 3: writes entities.json directly (new code)
    └── Stage 4: loads KnowledgeGraph (old code) to write triplets
            └── calls KnowledgeGraph.add_triplets_from_metadata()
                    └── calls disambiguation.py (old placeholder system)
```

Eventually `constructor/` should be absorbed into `construct/`:
- `KnowledgeGraph` → simplified to just a save/load container, or removed entirely
- `disambiguation.py` → replaced by `construct/entity_lut.py` (already written, not yet wired)

For now both coexist because the triplet build stage still relies on the old `KnowledgeGraph.add_triplets_from_metadata` flow which handles entity discovery from FDC food/chemical names and the placeholder disambiguation.

## File structure

```
src/construct/
  __init__.py
  runner.py                  # ConstructRunner — orchestrates all stages
  corrections_applier.py     # Stage 1: apply corrections.yaml
  subtree_filter.py          # Stage 2: DFS-based ontology filtering
  entity_resolver.py         # Stage 3: three-pass orchestrator
  resolve_primary.py         # Stage 3, Pass 1: primary entities
  resolve_secondary.py       # Stage 3, Pass 2+3: linking + unlinked
  entity_lut.py              # Ambiguity-aware synonym lookup (replaces placeholders)
  triplet_builder.py         # Stage 4: orchestrator
  triplets/
    food_ontology.py         # Stage 4: food is_a triplets
    chemical_ontology.py     # Stage 4: chemical is_a triplets
    food_chemical.py         # Stage 4: food-chemical contains triplets
    flavor.py                # Stage 4: flavor descriptions
```
