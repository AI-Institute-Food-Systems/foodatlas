# FoodAtlas KGC

Knowledge graph construction pipeline for FoodAtlas. Ingests data from external sources (FoodOn, ChEBI, FDC, CTD, FlavorDB, etc.) and builds a unified knowledge graph of foods, chemicals, and diseases.

## Getting Started

```bash
# Install dependencies
uv sync

# Run full pipeline
uv run python main.py run

# Run a single stage (by name or number)
uv run python main.py run --stage ingest
uv run python main.py run --stage 1

# Run specific ingest sources
uv run python main.py run --stage ingest --source foodon --source chebi

# Shortcut: run ingest + entity resolution
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
  init  Shortcut: run ingest + entity resolution
```

### Configuration

Settings use Pydantic with env prefix `KGC_` (e.g. `KGC_DATA_DIR`). Key fields:

| Setting | Default | Description |
|---------|---------|-------------|
| `kg_dir` | `outputs/kg` | Output knowledge graph directory |
| `data_dir` | `data` | Raw data source directory |
| `output_dir` | `outputs` | General output directory |
| `cache_dir` | `outputs/cache` | Cache directory |
| `ncbi_email` | — | Required for NCBI Taxonomy queries |
| `pubchem_mapping_file` | — | PubChem synonym mapping |

Override via environment variables, config JSON, or CLI flags.

## Pipeline Stages

| # | Stage | Description |
|---|-------|-------------|
| 0 | `ingest` | Parse external sources into standardized parquet (nodes, edges, xrefs) |
| 1 | `entities` | Subtree filter + 3-pass entity resolution (food, chemical, disease) |
| 2 | `triplets` | Build ontology is_a, FDC contains, flavor descriptions; expand from IE metadata |
| 3 | `postprocessing` | Common names, grouping, synonym display (deferred) |

### Two-Phase Architecture

**Phase 1 — Ingest** (`src/ingest/`): Source adapters parse raw data files into a standardized parquet format with `nodes`, `edges`, and `xrefs` DataFrames per source. Each adapter implements the `SourceAdapter` protocol.

**Phase 2 — Construct** (`src/construct/`): Builds the KG from Phase 1 output:
- **EntityRunner**: Loads ingest parquet → filters ontology subtrees (DFS) → 3-pass entity resolution (primary from authoritative sources, link secondary via xrefs, create unlinked) → saves entities + LUTs.
- **TripletRunner**: Loads ingest parquet → builds ontology triplets (food is_a, chemical is_a) + FDC contains + flavor descriptions → expands from IE metadata if available → saves.

Corrections are loaded but not applied to the base KG — all fixes are read-time patches (overlay pattern).

## Architecture

### Models → Schema → Stores

Pydantic models in `src/models/` are the single source of truth for the KG schema. `src/stores/schema.py` derives column definitions from model fields (respecting aliases).

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
| `r5` | HAS_FLAVOR (deprecated — flavors stored as chemical descriptions) |

### Output Files

| File | Description |
|------|-------------|
| `entities.json` | All entities (food, chemical, disease) |
| `triplets.json` | Relationship triplets (head, rel, tail) |
| `metadata_contains.json` | Food-chemical concentration metadata |
| `lookup_table_food.json` | Synonym → food entity ID mapping |
| `lookup_table_chemical.json` | Synonym → chemical entity ID mapping |
| `relationships.json` | Relationship type definitions |
| `food_ontology.json` | FoodOn is_a hierarchy |
| `chemical_ontology.json` | ChEBI hierarchy |

## Project Structure

```
kgc/
├── main.py                        # CLI entry point (Click)
├── pyproject.toml
├── src/
│   ├── config/                    # Settings, defaults.json, corrections.yaml
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
│   │   ├── stages.py              # PipelineStage enum (0-3)
│   │   └── runner.py              # PipelineRunner
│   ├── ingest/                    # Phase 1: source adapters
│   │   ├── protocol.py            # SourceAdapter protocol + parquet schema
│   │   ├── runner.py              # IngestRunner
│   │   └── adapters/              # Per-source adapters (foodon, chebi, fdc, ...)
│   ├── construct/                 # Phase 2: entity resolution + triplet building
│   │   ├── entity_runner.py       # EntityRunner (ENTITIES stage)
│   │   ├── triplet_runner.py      # TripletRunner (TRIPLETS stage)
│   │   ├── ingest_loader.py       # Shared Phase 1 parquet loader
│   │   ├── entity_resolver.py     # 3-pass entity resolution
│   │   ├── entity_lut.py          # Ambiguity-aware synonym LUT
│   │   ├── resolve_primary.py     # Pass 1: authoritative sources
│   │   ├── resolve_secondary.py   # Pass 2+3: link + create unlinked
│   │   ├── subtree_filter.py      # Ontology subtree filtering (DFS)
│   │   ├── triplet_builder.py     # Orchestrates triplet creation
│   │   └── triplets/              # Per-domain triplet builders
│   ├── constructor/               # Core KG building
│   │   ├── knowledge_graph.py     # KG orchestrator (load/save all stores)
│   │   └── disambiguation.py      # Entity synonym resolution
│   ├── discovery/                 # Runtime entity creation
│   │   ├── query.py               # API stubs for NCBI & PubChem
│   │   ├── food.py                # Food entities from NCBI Taxonomy
│   │   └── chemical.py            # Chemical entities from PubChem
│   ├── integration/               # Scaffold for empty KG files
│   ├── preprocessing/             # Text normalization (names, concentrations, units)
│   ├── postprocessing/            # Common names, synonyms display, grouping
│   └── utils/                     # Shared helpers
├── data/                          # Reference data sources
├── outputs/                       # Generated output directory
└── tests/
```

## Data Sources

See [`data/README.md`](data/README.md) for details on external data sources:
FoodOn, ChEBI, CDNO, FDC, CTD, FlavorDB, FlavorGraph, HSDB, Lit2KG, MeSH, PubChem.
