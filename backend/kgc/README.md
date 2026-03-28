# FoodAtlas KGC

Knowledge graph construction pipeline for FoodAtlas. Ingests data from 11 external sources (FoodOn, ChEBI, FDC, CTD, FlavorDB, etc.) and builds a unified knowledge graph of foods, chemicals, and diseases. Flavor descriptors from FlavorDB are stored as descriptions on chemical entities rather than as separate entities.

## Getting Started

```bash
# Install dependencies
uv sync

# Run full pipeline
uv run python main.py run

# Run a single stage (by name or 0-indexed number)
uv run python main.py run --stage 0
uv run python main.py run --stage data_cleaning

# Shortcut: initialize KG only
uv run python main.py init

# Run tests
uv run pytest
```

### CLI Options

```
uv run python main.py [OPTIONS] COMMAND [ARGS]

Options:
  --config FILE       Path to config JSON (overrides defaults.json)
  -v, --verbose       Enable DEBUG logging

Commands:
  run   Run pipeline stages (--stage repeatable, omit for all)
  init  Shortcut: run entity and triplet initialization only
```

### Configuration

Settings use Pydantic with env prefix `KGC_` (e.g. `KGC_DATA_DIR`). Key fields:

| Setting | Default | Description |
|---------|---------|-------------|
| `kg_dir` | `data/kg` | Output knowledge graph directory |
| `data_dir` | `data` | Raw data source directory |
| `output_format` | `jsonl` | Serialization format |
| `ncbi_email` | — | Required for NCBI Taxonomy queries |
| `pubchem_mapping_file` | — | PubChem synonym mapping |

Override via environment variables, config JSON, or CLI flags.

## Pipeline Stages

| # | Stage | Description |
|---|-------|-------------|
| 0 | `data_cleaning` | Process external sources (FoodOn, ChEBI, CDNO, MeSH, PubChem, CTD, FlavorDB) |
| 1 | `entity_init` | Create entities from external sources (food, chemical, disease) |
| 2 | `triplet_init` | Create ontology triplets, merge FDC/CTD triplets, apply flavor descriptions |
| 3 | `metadata_processing` | Handled by the IE pipeline (external) |
| 4 | `triplet_expansion` | Add triplets from extracted metadata |
| 5 | `postprocessing` | Apply common names, synonyms display, group chemicals/foods |

## Architecture

### Models → Schema → Stores

Pydantic models in `src/models/` are the single source of truth for the KG schema.
`src/stores/schema.py` derives column definitions from model fields (respecting aliases),
so column definitions are never duplicated.

- **Models** define field names, types, defaults, and aliases
- **Schema** auto-derives column lists from models
- **Stores** wrap pandas DataFrames with JSON I/O and lookup tables (synonym → entity ID)
- **Discovery** creates new entities at runtime by querying NCBI Taxonomy (food) or PubChem (chemical)

### Relationship Types

| ID | Type |
|----|------|
| `r1` | CONTAINS (food → chemical) |
| `r2` | IS_A (ontology hierarchy) |
| `r3` | POSITIVELY_CORRELATES_WITH (chemical → disease) |
| `r4` | NEGATIVELY_CORRELATES_WITH (chemical → disease) |
| `r5` | HAS_FLAVOR (deprecated — flavors are now stored as chemical descriptions) |

### Output Files

| File | Description |
|------|-------------|
| `entities.json` | All entities (food, chemical, disease); chemicals may include `_flavor_descriptions` |
| `triplets.json` | Relationship triplets (head, rel, tail) |
| `metadata_contains.json` | Food-chemical concentration metadata |
| `metadata_disease.json` | Chemical-disease metadata |
| `metadata_flavor.json` | Chemical-flavor metadata (legacy, no longer generated) |
| `lookup_table_food.json` | Synonym → food entity ID mapping |
| `lookup_table_chemical.json` | Synonym → chemical entity ID mapping |
| `relationships.json` | Relationship type definitions |
| `retired.json` | Merged/deprecated entities |
| `food_ontology.json` | FoodOn is_a hierarchy |
| `chemical_ontology.json` | ChEBI/CDNO hierarchy |

## Project Structure

```
kgc/
├── main.py                        # CLI entry point (Click)
├── pyproject.toml
├── src/
│   ├── config/                    # Settings and defaults.json
│   ├── models/                    # Pydantic data models
│   │   ├── entity.py              # Entity (food/chemical/disease)
│   │   ├── relationship.py        # RelationshipType enum
│   │   ├── triplet.py             # Triplet (head, rel, tail)
│   │   ├── metadata.py            # MetadataContains/Disease/Flavor
│   │   ├── settings.py            # KGCSettings (env prefix KGC_)
│   │   └── version.py             # Version tracking
│   ├── stores/                    # Runtime DataFrame containers
│   │   ├── schema.py              # Column defs derived from models
│   │   ├── entity_store.py        # EntityStore with synonym LUTs
│   │   ├── triplet_store.py       # TripletStore with deduplication
│   │   └── metadata_store.py      # MetadataContainsStore
│   ├── pipeline/                  # Execution orchestration
│   │   ├── stages.py              # PipelineStage enum (0-5)
│   │   └── runner.py              # PipelineRunner
│   ├── constructor/               # Core KG building
│   │   ├── knowledge_graph.py     # KG orchestrator (load/save all stores)
│   │   └── disambiguation.py      # Entity synonym resolution
│   ├── discovery/                 # Runtime entity creation
│   │   ├── query.py               # API stubs for NCBI & PubChem
│   │   ├── food.py                # Food entities from NCBI Taxonomy
│   │   └── chemical.py            # Chemical entities from PubChem
│   ├── integration/               # External data source ingestion
│   │   ├── scaffold.py            # Create empty KG files
│   │   ├── entities/              # Entity loaders (food, chemical, disease)
│   │   ├── ontologies/            # Hierarchy processing (FoodOn, ChEBI, CDNO, MeSH)
│   │   └── triplets/              # Relationship loaders (FDC, CTD) and flavor descriptions
│   ├── preprocessing/             # Text normalization (names, concentrations, units)
│   ├── postprocessing/            # Common names, synonyms display, grouping
│   └── utils/                     # Shared helpers
├── data/                          # Reference data sources (see data/README.md)
├── examples/                      # Sample inputs and KG outputs
├── outputs/                       # Generated output directory
└── tests/                         # 36 test files, 80% coverage minimum
```

## Data Sources

See [`data/README.md`](data/README.md) for details on external data sources:
FoodOn, ChEBI, CDNO, FDC, CTD, FlavorDB, FlavorGraph, HSDB, Lit2KG, MeSH, PubChem.
