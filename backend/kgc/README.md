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
uv run python main.py run --stage 0

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

The pipeline writes parquet files to `outputs/kg/` (the loadable knowledge graph) plus sidecars in `outputs/kg/{checkpoints,diagnostics,intermediate}/` and per-source ingestion outputs in `outputs/ingest/`.

| File | Description |
|------|-------------|
| `entities.parquet` | All entities (food, chemical, disease) |
| `entity_registry.parquet` | Persistent (source, native_id) → foodatlas_id mapping for stable IDs |
| `triplets.parquet` | Relationship triplets (head, rel, tail) |
| `relationships.parquet` | Relationship type definitions (`r1`–`r5`) |
| `evidence.parquet` | Per-triplet evidence rows |
| `attestations.parquet` | Per-triplet attestation/provenance |
| `attestations_ambiguous.parquet` | Attestations whose entity resolution was ambiguous |
| `retired.parquet` | Entities retired in the latest run (for downstream reconciliation) |

## Project Structure

```
kgc/
├── main.py                        # CLI entry point (Click)
├── pyproject.toml
├── src/
│   ├── config/                    # Settings, defaults.json, corrections.yaml
│   ├── models/                    # Pydantic data models
│   ├── stores/                    # Runtime DataFrame containers (EntityStore, TripletStore, MetadataStore)
│   ├── utils/                     # Shared helpers (JSON I/O, constants)
│   └── pipeline/                  # Orchestration + stage implementations
│       ├── stages.py              # PipelineStage enum (0-3)
│       ├── runner.py              # PipelineRunner orchestrator
│       ├── scaffold.py            # Create empty KG files
│       ├── ingest_loader.py       # Shared Phase 1 parquet loader
│       ├── ingest/                # Stage 0: source adapters
│       │   ├── protocol.py        # SourceAdapter protocol + parquet schema
│       │   ├── runner.py          # IngestRunner
│       │   └── adapters/          # Per-source (foodon, chebi, fdc, ctd, ...)
│       ├── entities/              # Stage 1: entity resolution
│       │   ├── runner.py          # EntityRunner
│       │   ├── resolver.py        # 3-pass entity resolution
│       │   ├── lut.py             # Ambiguity-aware synonym LUT
│       │   ├── resolve_primary.py # Pass 1: authoritative sources
│       │   ├── resolve_secondary.py # Pass 2+3: link + create unlinked
│       │   └── subtree_filter.py  # Ontology subtree filtering (DFS)
│       ├── triplets/              # Stage 2: triplet construction
│       │   ├── runner.py          # TripletRunner
│       │   ├── builder.py         # Orchestrates triplet creation
│       │   ├── knowledge_graph.py # KG container (load/save all stores)
│       │   ├── food_ontology.py   # Food is_a triplets
│       │   ├── chemical_ontology.py # Chemical is_a triplets
│       │   ├── food_chemical.py   # Food-chemical CONTAINS triplets
│       │   └── flavor.py          # Flavor descriptions
│       └── postprocessing/        # Stage 3: deferred
├── data/                          # Reference data sources
├── outputs/                       # Generated output directory
└── tests/
```

## Data Sources

See [`data/README.md`](data/README.md) for details on external data sources:
FoodOn, ChEBI, CDNO, FDC, CTD, FlavorDB, FlavorGraph, HSDB, MeSH, PubChem.

> `data/Lit2KG/` is **deprecated** — the pipeline no longer reads from it. The legacy text-parser outputs remain on disk for historical reference and are excluded from the S3 sync. The `"lit2kg:..."` source tag in `src/pipeline/ie/loader.py` is a provenance label, not a filesystem path; the actual literature inputs now flow from [`backend/ie/`](../ie/).

## Publishing & Loading (production)

Local KGC runs produce parquet under `outputs/kg/`. To publish to AWS and load into RDS:

```bash
# Publish source ontologies (only when registries refresh — quarterly)
./scripts/sync-data-to-s3.sh

# Publish the KGC pipeline output (after each run)
./scripts/sync-outputs-to-s3.sh

# Load the published outputs into RDS via a one-off ECS task
cd ../../infra/cdk && ./scripts/run-data-load.sh
```

Each sync creates an immutable timestamped directory under `s3://<bucket>/data/<UTC-ts>/` or `s3://<bucket>/outputs/<UTC-ts>/` and updates a `LATEST` pointer file. Old versions stay forever for traceability and rollback. See [`infra/README.md#s3-layout`](../../infra/README.md#s3-layout) for the full layout and [`infra/README.md#helper-scripts`](../../infra/README.md#helper-scripts) for the script catalog.

To pull a previous run as the baseline for the next KGC build:

```bash
# Source ontologies from the latest published version
./scripts/pull-data-from-s3.sh

# Previous KG outputs as PreviousFAKG/ baseline
./scripts/pull-from-s3.sh
```
