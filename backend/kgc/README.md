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
| 0 | `ingest` | Parse external sources into standardized parquet (`nodes`, `edges`, `xrefs`) per source |
| 1 | `entities` | Subtree filter + multi-pass entity resolution (food, chemical, disease) |
| 2 | `triplets` | Build ontology `is_a` (food/chemical/disease) + composition (`food → chemical`) + chemical–disease correlation triplets from ingest edges |
| 3 | `ie` | Fold IE TSV output (from `backend/ie/`) into the KG: parse concentrations, resolve raw food/chemical names, attach attestations |
| 4 | `enrichment` | Chemical/food classification (ChEBI/FoodOn-driven), grouping, common names, synonym display, flavor descriptions |

### Architecture in two phases

**Phase 1 — Ingest** (`src/pipeline/ingest/`): Source adapters parse raw data files into a standardized parquet format with `nodes`, `edges`, and `xrefs` per source. Each adapter implements the `SourceAdapter` protocol. Output lives under `outputs/ingest/<source>/`.

**Phase 2 — Construct** (the `entities`, `triplets`, `ie`, and `enrichment` stages): Builds the KG from Phase 1 output, writing checkpoints between stages so each stage can be re-run independently.

- **EntityRunner**: Loads ingest sources → filters ontology subtrees → multi-pass resolution (primary from authoritative sources; secondary linked via xrefs; unlinked entities created) → saves entities + lookup tables.
- **TripletRunner**: Loads ingest sources → walks ingest edges to build typed triplets (`food_food`, `food_chemical`, `chemical_chemical`, `chemical_disease`, `disease_disease`) → writes `triplets.parquet` + per-triplet `evidence.parquet` and `attestations.parquet`.
- **IERunner**: Reads `backend/ie/outputs/extraction/<date>/extraction_predicted.tsv`, parses concentrations, resolves food/chemical names against the entity LUTs, and folds new triplets + attestations into the KG. Ambiguous resolutions are written separately to `attestations_ambiguous.parquet`.
- **Enrichment**: Adds derived attributes (chemical classification, food classification, synonym display, flavors) and grouping artifacts. Writes outputs into `outputs/kg/intermediate/` and back-fills entity rows.

Manual corrections live in `src/config/corrections.yaml` and are loaded by `EntityRunner` during Phase 2. **Phase 1 ingest stays faithful** to the source data (no IDs dropped, no xrefs remapped) — corrections only kick in once construct starts. This separation keeps ingest reproducible and makes corrections auditable in one place.

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
| `CHANGELOG.md` | Auto-generated diff vs. the previous run (consumed by `publish-bundle.sh`) |
| `SUMMARY.md` | Human-readable summary of the run (entity/triplet counts, source coverage) |

## Project Structure

```
kgc/
├── main.py                        # CLI entry point (Click)
├── pyproject.toml
├── src/
│   ├── config/                    # KGCSettings, defaults.json, corrections.yaml,
│   │                              # foodatlas_classifications.yaml
│   ├── models/                    # Pydantic data models (entity, triplet, evidence, …)
│   ├── stores/                    # Runtime DataFrame containers + parquet schema
│   ├── utils/                     # Shared helpers (timing, orphans, IO, constants)
│   └── pipeline/                  # Orchestration + stage implementations
│       ├── stages.py              # PipelineStage enum
│       ├── runner.py              # PipelineRunner — dispatches stage handlers
│       ├── scaffold.py            # Create empty KG files
│       ├── checkpoint.py          # Per-stage checkpoint save/load
│       ├── knowledge_graph.py     # KG container (load/save all stores)
│       ├── load_sources.py        # Shared ingest-parquet loader for downstream stages
│       ├── ingest/                # Stage 0: source adapters
│       │   ├── protocol.py        # SourceAdapter protocol + parquet schema
│       │   ├── runner.py          # IngestRunner
│       │   └── adapters/          # foodon, chebi, cdno, fdc, ctd, dmd, flavordb,
│       │                          # mesh, pubchem
│       ├── entities/              # Stage 1: entity resolution
│       │   ├── runner.py          # EntityRunner
│       │   ├── resolver.py        # Multi-pass orchestrator
│       │   ├── resolve_primary.py # Pass 1: authoritative sources
│       │   ├── resolve_secondary.py # Pass 2+: link via xrefs, create unlinked
│       │   ├── resolve_dmd.py     # DMD-specific resolution helpers
│       │   ├── resolve_dmd_helpers.py
│       │   └── utils/             # Subtree filter, lookup-table builders, etc.
│       ├── triplets/              # Stage 2: typed triplet builders
│       │   ├── runner.py          # TripletRunner
│       │   ├── builder.py         # Dispatches per-relationship builders
│       │   ├── ambiguity.py       # Ambiguous-attestation handling
│       │   ├── food_food/         # Food is_a (FoodOn)
│       │   ├── food_chemical/     # Composition (FDC, DMD)
│       │   ├── chemical_chemical/ # Chemical is_a (ChEBI, CDNO, DMD, foodatlas)
│       │   ├── chemical_disease/  # Correlations (CTD)
│       │   └── disease_disease/   # Disease is_a (MeSH)
│       ├── ie/                    # Stage 3: fold IE TSV into the KG
│       │   ├── runner.py          # IERunner
│       │   ├── loader.py          # Read + standardize IE raw output
│       │   ├── conc_parser.py     # Concentration parsing/conversion
│       │   ├── resolver.py        # Resolve raw food/chemical names against LUTs
│       │   └── report.py          # Per-run resolution report
│       ├── enrichment/            # Stage 4: derived attributes
│       │   ├── classification.py
│       │   ├── food_classification.py
│       │   ├── flavor.py
│       │   ├── common_name.py
│       │   ├── synonyms_display.py
│       │   └── grouping/          # Chemical / food / MeSH grouping
│       └── report/                # Diff against previous KG (CHANGELOG.md generator)
├── data/                          # Reference data sources (see data/README.md)
├── outputs/                       # Generated output directory
│   ├── ingest/                    # Per-source Phase 1 outputs
│   └── kg/                        # The loadable KG + checkpoints/diagnostics/intermediate
├── scripts/                       # Sync + publish helpers (see below)
└── tests/
```

## Data Sources

See [`data/README.md`](data/README.md) for details on external data sources:
FoodOn, ChEBI, CDNO, FDC, CTD, DMD, FlavorDB, FlavorGraph, HSDB, MeSH, PubChem.

> `data/Lit2KG/` is **deprecated** — the pipeline no longer reads from it. The legacy text-parser outputs remain on disk for historical reference and are excluded from the S3 sync. The `"lit2kg:..."` source tag in `src/pipeline/ie/loader.py` is a provenance label, not a filesystem path; the actual literature inputs now flow from [`backend/ie/`](../ie/).

## Publishing & Loading (production)

Local KGC runs produce parquet under `outputs/kg/`. To publish to AWS and load into RDS:

```bash
# Publish source ontologies (only when registries refresh — quarterly)
./scripts/sync-data-to-s3.sh

# Publish the KGC pipeline output (after each run)
./scripts/sync-outputs-to-s3.sh

# Load the published outputs into RDS via a one-off ECS task
cd ../../infra/aws && ./scripts/run-data-load.sh
```

Each sync creates an immutable timestamped directory under `s3://<bucket>/data/<UTC-ts>/` or `s3://<bucket>/outputs/<UTC-ts>/` and updates a `LATEST` pointer file. Old versions stay forever for traceability and rollback. See [`infra/README.md#s3-layout`](../../infra/README.md#s3-layout) for the full layout and [`infra/README.md#helper-scripts`](../../infra/README.md#helper-scripts) for the script catalog.

To pull a previous run as the baseline for the next KGC build:

```bash
# Source ontologies from the latest published version
./scripts/pull-data-from-s3.sh

# Previous KG outputs as PreviousFAKG/ baseline
./scripts/pull-from-s3.sh
```

### Releasing a public bundle

`./scripts/publish-bundle.sh <version> <summary-file> [--kgc-run <id>] [--release-date <YYYY-MM-DD>]` packages the parquet files + `CHANGELOG.md` + a release `SUMMARY.md` from a KGC run already in the private bucket and uploads them to the public **downloads** bucket (`s3://<downloads>/bundles/foodatlas-<version>/`). It also updates `bundles/index.json`, the manifest the API's `/download` endpoint reads. Each entry there is traceable back to its originating `kgc_run` timestamp.

The CLI also exposes a one-off command for regenerating run diagnostics without re-running the pipeline:

```bash
uv run python main.py diagnostics
```
