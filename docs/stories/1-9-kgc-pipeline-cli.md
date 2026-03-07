# Story 1-9: Pipeline Runner + CLI

## Goal

Replace the 7 numbered shell scripts with a Python pipeline runner and Click CLI entry point. This makes the pipeline reproducible, configurable, and deployable.

## Depends On

- Stories 1-1 through 1-8 (all modules must be ported)

## Acceptance Criteria

- [ ] `pipeline/stages.py` — `PipelineStage` enum defining all stages:
  - `ONTOLOGY_PREP` (maps to `scripts/00_run_data_processing.sh`) — preprocess ontology data files (FoodOn, ChEBI, CDNO, MeSH, PubChem) via `integration/ontologies/`
  - `KG_INIT` (maps to `scripts/0_run_kg_init.sh`) — scaffold empty KG, initialize food/chemical entities, create ontologies, merge FDC nutrients
  - `METADATA_PROCESSING` (maps to `scripts/1_run_metadata_processing.sh`) — preprocess IE extraction output via `preprocessing/`
  - `TRIPLET_EXPANSION` (maps to `scripts/2_run_adding_triplets_from_metadata.sh`) — discover new entities via `discovery/`, add triplets via `KnowledgeGraph.add_triplets_from_metadata()`
  - `POSTPROCESSING` (maps to `scripts/3_run_postprocessing.sh`) — group entities, select common names, format synonyms via `postprocessing/`
  - `MERGE_DISEASE` (maps to `scripts/4_run_merging_disease.sh`) — import disease entities via `integration/entities/disease/`, then merge CTD triplets via `integration/triplets/ctd`
  - `MERGE_FLAVOR` (maps to `scripts/5_run_merging_flavor.sh`) — import flavor entities via `integration/entities/flavor/`, then merge FlavorDB triplets via `integration/triplets/flavordb`
- [ ] `pipeline/runner.py` — `PipelineRunner` class
  - `__init__(settings: KGCSettings)`
  - `run(stages: list[PipelineStage] | None = None)` — run all or selected stages in order
  - `run_stage(stage: PipelineStage)` — run a single stage
  - Internally creates a `KnowledgeGraph(settings)` instance and passes it to stage functions
  - Logging for stage start/end/duration
  - Validation check after `TRIPLET_EXPANSION` (matching original `test_kg` call in script 2)
- [ ] `main.py` — Click CLI with commands:
  - `run` — run pipeline stages (`--stage` option, repeatable)
  - `init` — shortcut for `KG_INIT` only
  - `--config` option to specify `config/defaults.json` override
  - `--input` option for IE extraction input file (used by `METADATA_PROCESSING`); accepts JSON or pkl, auto-detected from extension
  - `--output-format` option (`json`, `jsonl`, `parquet`; default: `jsonl`); sets `KGCSettings.output_format`
- [ ] All files pass `ruff check` and `mypy`
- [ ] All files under 300 lines
- [ ] Tests for:
  - Stage enum completeness (all 7 stages present)
  - Runner executes stages in order
  - CLI parses arguments correctly
  - `--stage` filtering works

## Stage-to-Module Mapping

| Stage | Shell Script | Modules Called |
|-------|-------------|---------------|
| `ONTOLOGY_PREP` | `00_run_data_processing.sh` | `integration.ontologies.foodon`, `.chebi`, `.cdno`, `.mesh`, `.pubchem` |
| `KG_INIT` | `0_run_kg_init.sh` | `integration.scaffold.create_empty_files`, `integration.entities.food.init_entities.append_foods_from_*`, `integration.entities.chemical.init_entities.append_chemicals_from_*`, `integration.ontologies.food`, `.chemical`, `integration.triplets.fdc.merge_fdc` |
| `METADATA_PROCESSING` | `1_run_metadata_processing.sh` | `preprocessing.*` (standardize names, concentrations, food parts from IE output) |
| `TRIPLET_EXPANSION` | `2_run_adding_triplets_from_metadata.sh` | `discovery.food.create_food_entities`, `discovery.chemical.create_chemical_entities`, `KnowledgeGraph.add_triplets_from_metadata` |
| `POSTPROCESSING` | `3_run_postprocessing.sh` | `postprocessing.grouping.chemicals`, `.foods`, `postprocessing.common_name`, `postprocessing.synonyms_display` |
| `MERGE_DISEASE` | `4_run_merging_disease.sh` | `integration.entities.disease.init_entities.append_diseases_from_ctd`, `integration.triplets.ctd.merge_ctd_triplets` |
| `MERGE_FLAVOR` | `5_run_merging_flavor.sh` | `integration.entities.flavor.init_entities.append_flavors_from_flavordb`, `integration.triplets.flavordb.merge_flavordb_triplets` |

## Source Files

| Target | Source |
|--------|--------|
| `pipeline/__init__.py` | New |
| `pipeline/stages.py` | New (replaces `scripts/0_run_kg_init.sh` through `scripts/5_run_merging_flavor.sh`) |
| `pipeline/runner.py` | New (orchestration logic extracted from shell scripts) |
| `main.py` | Replaces existing stub |

## Notes

- Each shell script maps to one `PipelineStage`. The runner calls the ported Python functions directly (not via `python -m`).
- The `KnowledgeGraph` instance is the central object passed between stages. Stages that need it (all except `ONTOLOGY_PREP` and `METADATA_PROCESSING`) receive the KG object.
- The runner should produce `version.json` at the end of a full pipeline run (timestamp, stages run, input file hash).
- Internal pipeline stages use DataFrames; serialization to the chosen output format happens at the final output step via `KnowledgeGraph.save()`.
- `ONTOLOGY_PREP` produces intermediate files consumed by `KG_INIT`. It can be skipped if those files already exist.
