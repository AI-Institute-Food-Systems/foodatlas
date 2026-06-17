# FoodAtlas KGC Pipeline — Detailed Reference

This document is a deep, code-level walkthrough of the Knowledge Graph Construction
(KGC) pipeline rooted at [backend/kgc/](.). It complements the high-level
[README.md](README.md) by tracing every stage from raw source files to the final
parquet bundle in `outputs/kg/`.

> Authoritative entry point: [main.py](main.py). All stage handlers are wired in
> [src/pipeline/runner.py](src/pipeline/runner.py).

---

## 1. End-to-end data flow

```
                                     ┌───────────────────────────────────────────┐
 data/  (raw sources)                │                                           │
   ├─ FoodOn / ChEBI / CDNO / CTD    │            outputs/kg/                    │
   ├─ FDC / DMD / MeSH / PubChem     │  entities.parquet                         │
   └─ FlavorDB / HSDB                │  entity_registry.parquet                  │
                │                    │  triplets.parquet                         │
                ▼                    │  relationships.parquet                    │
       ┌────────────────────┐        │  evidence.parquet                         │
       │ Stage 0: INGEST    │──────► outputs/ingest/<source>/*.parquet           │
       │ src/pipeline/ingest│        │  attestations.parquet                     │
       └────────────────────┘        │  attestations_ambiguous.parquet           │
                │                    │  intermediate/lookup_table_*.json         │
                ▼                    │  checkpoints/<stage>/*                    │
       ┌────────────────────┐        │  diagnostics/*.jsonl|tsv                  │
       │ Stage 1: ENTITIES  │        │                                           │
       │   subtree filter   │        └───────────────────────────────────────────┘
       │   3-pass resolver  │
       └────────────────────┘
                │
                ▼
       ┌────────────────────┐
       │ Stage 2: TRIPLETS  │   ontology is_a, FDC contains, CTD assoc, DMD…
       └────────────────────┘
                │
                ▼
       ┌────────────────────┐
       │ Stage 3: IE        │   LLM-extracted (food → chemical) attestations
       └────────────────────┘
                │
                ▼
       ┌────────────────────┐
       │ Stage 4: ENRICHMENT│   classifications, common names, flavor, grouping
       └────────────────────┘
```

Every stage boundary saves a full checkpoint copy of the seven KG parquets +
lookup tables under `outputs/kg/checkpoints/<stage_name>/`, so any stage can be
re-run independently.

---

## 2. CLI surface ([main.py](main.py))

Click app with one global option group and four commands.

**Global options**

- `--config FILE` — JSON file merged into [`KGCSettings`](src/models/settings.py) on top
  of [`src/config/defaults.json`](src/config/defaults.json).
- `-v, --verbose` — DEBUG-level logging.

**Commands**

| Command | Purpose |
|--------|---------|
| `run [--stage SPEC]... [--source NAME]...` | Run pipeline stages. `--stage` accepts a name (`ingest`), index (`0`), or range (`1:3`); repeatable. `--source` filters the ingest stage to specific adapters. Omit both to run everything. |
| `init` | Shortcut: `run --stage ingest --stage entities` — the bootstrap path for fresh repos. |
| `diagnostics` | Regenerates `diagnostics/kgc_orphans.jsonl` and `kgc_unclassified.jsonl` from an existing KG without re-running stages. |
| `report [--output FILE] [--format markdown|text]` | Diffs the current KG against `data/PreviousFAKG/v3.3/`. `markdown` produces release-style CHANGELOG (default `outputs/kg/CHANGELOG.md`); `text` is an operator console report. |

`_resolve_stages()` ([main.py:34](main.py)) parses the stage spec into sorted
[`PipelineStage`](src/pipeline/stages.py) enum members.

`KGCSettings` ([src/models/settings.py](src/models/settings.py)) is a Pydantic
`BaseSettings` with env prefix `KGC_` (e.g. `KGC_DATA_DIR`, `KGC_KG_DIR`).
A `model_validator` merges `defaults.json` before env vars override.

---

## 3. Orchestration

### `PipelineStage` ([src/pipeline/stages.py](src/pipeline/stages.py))

```
INGEST      = 0
ENTITIES    = 1
TRIPLETS    = 2
IE          = 3
ENRICHMENT  = 4
```

### `PipelineRunner` ([src/pipeline/runner.py](src/pipeline/runner.py))

- `run(stages=None, sources=None)` sorts stages by `.value` and dispatches via
  `_STAGE_HANDLERS`, a `{PipelineStage: bound_method}` map.
- Per-stage handlers (`_run_ingest`, `_run_entities`, `_run_triplets`,
  `_run_ie`, `_run_enrichment`) each instantiate their stage runner with the
  active settings and call `.run(...)`.
- The enrichment handler is the one stage that re-hydrates the KG from a
  checkpoint (the `ie` checkpoint) before mutating it.

### Scaffolding ([src/pipeline/scaffold.py](src/pipeline/scaffold.py))

Creates the empty parquet skeleton when the KG directory is fresh:

- `ensure_registry_exists()` — seeds `entity_registry.parquet` from
  `previous_kg_entities` TSV when present (via
  [`seed_registry`](src/stores/registry_seeder.py)), otherwise writes an empty
  registry.
- `create_empty_entity_files()` — writes empty `entities.parquet` plus the two
  `intermediate/lookup_table_*.json` files.
- `create_empty_triplet_files()` — writes `triplets.parquet`, `evidence.parquet`,
  `attestations.parquet`, and seeds `relationships.parquet` with every
  `RelationshipType` enum row.

### Checkpoints ([src/pipeline/checkpoint.py](src/pipeline/checkpoint.py))

- `save_checkpoint(kg_dir, stage_name)` copies all seven KG parquets plus the
  two lookup-table JSONs into `kg_dir/checkpoints/<stage_name>/`.
- `load_checkpoint(kg_dir, stage_name)` restores them in place (used by the
  enrichment stage and by ad-hoc re-runs).

---

## 4. Stage 0 — Ingest ([src/pipeline/ingest/](src/pipeline/ingest/))

Each external source becomes three standardized DataFrames written to
`outputs/ingest/<source_id>/`:

| DataFrame | Columns (see [protocol.py](src/pipeline/ingest/protocol.py)) |
|-----------|--------------------------------------------------------------|
| `*_nodes.parquet` | `source_id`, `native_id`, `name`, `synonyms`, `synonym_types`, `node_type`, `raw_attrs` |
| `*_edges.parquet` | `source_id`, `head_native_id`, `tail_native_id`, `edge_type`, `raw_attrs` |
| `*_xrefs.parquet` | `source_id`, `native_id`, `target_source`, `target_id` |

Plus a `*_manifest.json` ([`SourceManifest`](src/models/ingest.py)) with file
hashes, counts, and timestamps.

### Adapter contract

```python
class SourceAdapter(Protocol):
    @property
    def source_id(self) -> str: ...
    def ingest(self, raw_dir: Path, output_dir: Path,
               progress: ProgressCallback = ...) -> SourceManifest: ...
```

### `IngestRunner` ([src/pipeline/ingest/runner.py](src/pipeline/ingest/runner.py))

- Filters `ALL_ADAPTERS` by the optional `--source` list.
- Runs adapters in a `multiprocessing.Pool` — one process per adapter.
- A queue collects `(source_id, current, total)` ticks; sentinels `-1` (done)
  and `-2` (error) close each tqdm bar.
- Logging is temporarily suppressed during the parallel phase to avoid
  interleaving.

### Adapters (per source)

| Adapter | Raw input | Output highlights |
|---|---|---|
| **foodon** ([foodon.py](src/pipeline/ingest/adapters/foodon.py)) | `FoodOn/foodon-synonyms.tsv` | One node per class, synonyms typed (`label`, `exact`, `narrow`, …); `is_a` edges; common name prefers the `label` type. |
| **chebi** ([chebi.py](src/pipeline/ingest/adapters/chebi.py)) | `compounds.tsv`, `names.tsv`, `relation.tsv` | Only root compounds (`PARENT_ID` null); attaches star rating to each node; edge types from `relation.tsv`. |
| **cdno** ([cdno.py](src/pipeline/ingest/adapters/cdno.py)) | `cdno.owl` (parsed with BeautifulSoup) | Strips the `concentration of … in material entity` wrapper from labels; emits `is_a` edges from `rdfs:subClassOf`; xrefs to ChEBI (`owl:equivalentClass`) and to FDC nutrient IDs (`oboInOwl:hasDbXref`). |
| **ctd** ([ctd.py](src/pipeline/ingest/adapters/ctd.py)) | `CTD_chemicals_diseases.csv`, `CTD_diseases.csv` | Disease nodes only; chemical-disease edges retained (filtered later to `DirectEvidence=true`). |
| **fdc** ([fdc.py](src/pipeline/ingest/adapters/fdc.py)) | USDA FDC dumps | Foods + nutrients as nodes; food→nutrient `contains` edges with concentrations in `raw_attrs`; xrefs into FoodOn and CDNO. |
| **dmd** ([dmd.py](src/pipeline/ingest/adapters/dmd.py)) | Dairy Molecules DB CSV | Parses Postgres `{a,"b"}` set notation; milk-specific concentration conversions; xrefs to ChEBI/PubChem. |
| **mesh** ([mesh.py](src/pipeline/ingest/adapters/mesh.py)) | `desc*.xml`, `supp*.xml` | Descriptor + supplementary records; tree-number hierarchy edges. |
| **pubchem** ([pubchem.py](src/pipeline/ingest/adapters/pubchem.py)) | `SID-Map` (15 GB), `CID-MeSH.txt` | Line-by-line scan; emits **xrefs only** — PubChem CID ↔ ChEBI and PubChem CID ↔ MeSH. |
| **flavordb** ([flavordb.py](src/pipeline/ingest/adapters/flavordb.py)) | `flavordb_scrape.json`, HSDB JSON | Fuzzy-matches HSDB compounds to FlavorDB flavor names; merges by `(source, pubchem_id, flavor, url)`. |

### Loader ([src/pipeline/load_sources.py](src/pipeline/load_sources.py))

`load_sources(ingest_dir)` walks `outputs/ingest/<source_id>/`, reads the three
parquets, deserializes `raw_attrs` JSON strings, and returns
`{source_id: {"nodes": df, "edges": df, "xrefs": df}}`. This is the single
input shape consumed by every downstream stage.

---

## 5. Stage 1 — Entities ([src/pipeline/entities/](src/pipeline/entities/))

### `EntityRunner` ([runner.py](src/pipeline/entities/runner.py))

1. `load_sources()` → all ingest parquets.
2. `filter_sources(sources, corrections)` → subtree-filtered copies.
3. `ensure_registry_exists()` → idempotent registry seeding.
4. `EntityResolver(...).resolve()` → 3-pass entity creation.
5. `entity_store.save()` → writes `entities.parquet` + lookup-table JSONs.
6. `save_checkpoint("entities")`.

### Subtree filtering ([utils/subtree_filter.py](src/pipeline/entities/utils/subtree_filter.py))

Each source has a DFS anchor configured in
[`OntologyRoots`](src/config/corrections.py):

- **FoodOn** — keep descendants of `foodon_is_food` ∪ `foodon_is_organism`;
  tags rows with boolean `is_food` / `is_organism` for later classification.
- **ChEBI** — keep descendants of `chebi_molecular_entity` (ID 23367); drops
  proteins / sequences.
- **CTD** — restrict chemical-disease edges to `DirectEvidence=true`.
- **CDNO** — keep nodes that either have an FDC nutrient xref or live in a
  whitelisted subtree (`cdno_keep_subtrees` in corrections).

`_compute_descendants()` builds a `child → parents` map and DFS-walks each
configured root.

### Three-pass resolution ([resolver.py](src/pipeline/entities/resolver.py))

**Pass 1 — Primary** ([resolve_primary.py](src/pipeline/entities/resolve_primary.py))

Authoritative sources mint new entities:

- `create_foods_from_foodon()` — `FoodEntity` per FoodOn node.
- `create_chemicals_from_chebi()` — `ChemicalEntity` with ChEBI ID + star.
- `create_diseases_from_ctd()` — `DiseaseEntity` per CTD disease.
- `create_chemicals_from_dmd()` — DMD-seeded chemicals.

For each row: check registry → reuse existing `foodatlas_id` or mint
`e{next_eid}` → register in registry → push into the entity LUT keyed by
lowercased synonyms.

**Pass 2 — Link secondary** ([resolve_secondary.py](src/pipeline/entities/resolve_secondary.py))

Xref-based attachment. No new IDs minted — these calls only append to
`external_ids` and register aliases:

- `link_cdno_to_chebi`, `link_fdc_foods_to_foodon`, `link_fdc_nutrients`,
  `link_dmd` — 1:1 mappings, registered in the registry.
- `link_pubchem_to_chebi`, `link_mesh_to_chebi`
  ([utils/link_xrefs.py](src/pipeline/entities/utils/link_xrefs.py)) — legitimately
  1:N, **only** added to `external_ids`, never registered.

**Pass 3 — Unlinked** (same module)

Creates entities for source rows that did not match any existing entity in
Pass 2: `create_unlinked_cdno`, `create_unlinked_fdc_foods`,
`create_unlinked_fdc_nutrients`, `create_unlinked_dmd`. Mints a new
`foodatlas_id`; if the row is ambiguous it suffixes the synonym with the
xref ID before LUT insertion.

DMD has a dedicated helper module
([resolve_dmd.py](src/pipeline/entities/resolve_dmd.py),
[resolve_dmd_helpers.py](src/pipeline/entities/resolve_dmd_helpers.py)) because
its xref topology and concentration semantics differ enough from the other
secondary sources.

### Ambiguity-aware LUT ([utils/lut.py](src/pipeline/entities/utils/lut.py))

`{(entity_type, name_lower) → [foodatlas_id, …]}`. APIs:

- `add(type, name, id)` — append.
- `lookup(type, name)` → list (length > 1 means ambiguous).
- `lookup_unique(type, name)` → returns id iff unambiguous.
- `ambiguous_entries(type)` → diagnostic helper.

Persisted as `intermediate/lookup_table_food.json` and
`lookup_table_chemical.json`.

### Entity registry ([src/stores/entity_registry.py](src/stores/entity_registry.py))

Persistent `(source, native_id) → [foodatlas_id, …]` map (1:N via
`register_alias`). `reassign()` updates the mapping when a seeded ID becomes
stale. `next_eid` tracks the next allocatable suffix.

Seeded from the previous KG by
[`registry_seeder.seed_registry`](src/stores/registry_seeder.py); diffed for
release notes by [`registry_diff.py`](src/stores/registry_diff.py).

---

## 6. Stage 2 — Triplets ([src/pipeline/triplets/](src/pipeline/triplets/))

### `TripletRunner` ([runner.py](src/pipeline/triplets/runner.py))

Loads the `entities` checkpoint + ingest sources, calls
[`build_triplets`](src/pipeline/triplets/builder.py), splits any ambiguous
attestations into `attestations_ambiguous.parquet`, and saves the `triplets`
checkpoint.

### Builder orchestration ([builder.py](src/pipeline/triplets/builder.py))

```python
merge_food_ontology(kg, sources)               # food_food/foodon.py     (r2 IS_A)
merge_chemical_ontology(kg, sources)           # chemical_chemical/chebi.py
merge_chemical_ontology_cdno(kg, sources)      # chemical_chemical/cdno.py
merge_chemical_ontology_dmd(kg, sources)       # chemical_chemical/dmd.py
merge_chemical_ontology_foodatlas(kg, sources) # chemical_chemical/foodatlas.py
merge_disease_ontology(kg, sources)            # disease_disease/ctd.py
merge_fdc_triplets(kg, sources)                # food_chemical/fdc.py    (r1 CONTAINS)
merge_ctd_triplets(kg, sources)                # chemical_disease/ctd.py (r3 / r4)
merge_dmd_triplets(kg, sources)                # food_chemical/dmd.py
```

Relationship IDs (see [src/models/relationship.py](src/models/relationship.py)):

| ID | Type |
|---|---|
| `r1` | CONTAINS (food → chemical) |
| `r2` | IS_A (ontology) |
| `r3` | POSITIVELY_CORRELATES_WITH |
| `r4` | NEGATIVELY_CORRELATES_WITH |
| `r5` | HAS_FLAVOR (deprecated; flavors now live in chemical attributes) |

### Shared utilities ([utils.py](src/pipeline/triplets/utils.py))

`explode_external_ids(entities, key)` expands the `external_ids` JSON column
into a long table `native_id → foodatlas_id (+ candidate list)`. Builders
join ingest edges against this table; rows where the native id maps to
multiple `foodatlas_id`s flow through [`ambiguity.py`](src/pipeline/triplets/ambiguity.py),
which records every candidate on the attestation so downstream curation can
disambiguate.

### Triplet store ([src/stores/triplet_store.py](src/stores/triplet_store.py))

Indexed by composite key `f"{head_id}_{rel}_{tail_id}"`. Two write paths:

- `add_ontology(df)` — triplet rows without attestation IDs.
- `add_metadata(df)` — triplet rows linked to attestation IDs.

Duplicate keys merge their attestation lists rather than re-inserting.

---

## 7. Stage 3 — IE integration ([src/pipeline/ie/](src/pipeline/ie/))

This stage folds LLM-extracted `(food → chemical)` claims into the KG.

### `IERunner` ([runner.py](src/pipeline/ie/runner.py))

1. Restore `triplets` checkpoint.
2. Clear `diagnostics/`.
3. Discover every IE input directory under `settings.ie_raw_dir` (each
   subfolder ships an `extraction_predicted.tsv` or `.json` plus a model id).
4. For each batch: `load_ie_raw → resolve_ie_metadata → metadata_to_attestations`.
5. Write `attestations_ambiguous.parquet` and orphan/unclassified diagnostics.
6. Save the `ie` checkpoint.

### Loader ([loader.py](src/pipeline/ie/loader.py))

Normalizes input rows (JSON, TSV, parquet) into a single dataframe with
`pmcid`, `section`, `matched_query`, `sentence`, `prob`, `response`. The model
response is parsed as a `(food, food_part, chemical, quantity)` tuple, with
Greek-letter normalization and punctuation cleanup. Parse failures land in
`diagnostics/ie_parse_errors.tsv`.

### Concentration parsing ([conc_parser.py](src/pipeline/ie/conc_parser.py))

Regex-based extraction of `(value, unit)` from free-text quantities, including
ranges and approximations. Weight-basis suffixes (`fw`, `dw`, `dm`) are stripped.
`convert_conc(value, unit)` normalizes to `mg/100g` via a hardcoded conversion
table; unrecognized units are dropped into `diagnostics/ie_conc_unconverted.tsv`.

### Resolver ([resolver.py](src/pipeline/ie/resolver.py))

Maps raw food and chemical names to `foodatlas_id`s via the entity LUTs. If a
name resolves to multiple IDs, the row is exploded into one triplet per
candidate, and every candidate is preserved on the attestation. Unresolved
names go to `diagnostics/ie_unresolved.jsonl`.

### Report ([report.py](src/pipeline/ie/report.py))

Counts of resolved / unresolved / ambiguous names per IE batch, written as a
structured log line.

---

## 8. Stage 4 — Enrichment ([src/pipeline/enrichment/](src/pipeline/enrichment/))

Runs after `ie`; loads the `ie` checkpoint, mutates entities, then saves the
final KG.

| Module | Effect |
|---|---|
| [classification.py](src/pipeline/enrichment/classification.py) | DFS from curated ChEBI root anchors (`flavonoid`, `tannin`, …) → adds chemical categories to `entity.attributes`. |
| [food_classification.py](src/pipeline/enrichment/food_classification.py) | Analogous FoodOn-based classification for foods. |
| [flavor.py](src/pipeline/enrichment/flavor.py) | Attaches FlavorDB / HSDB flavor descriptors to chemical entities. |
| [common_name.py](src/pipeline/enrichment/common_name.py) | Picks display-friendly common names from typed synonyms. |
| [synonyms_display.py](src/pipeline/enrichment/synonyms_display.py) | Sorts & dedupes synonyms for the UI surface. |
| [grouping/foods.py](src/pipeline/enrichment/grouping/foods.py), [chemicals.py](src/pipeline/enrichment/grouping/chemicals.py), [mesh.py](src/pipeline/enrichment/grouping/mesh.py) | Computes display groups for the frontend's grouped views. |
| [utils.py](src/pipeline/enrichment/utils.py) | Shared traversal helpers. |

---

## 9. Stores ([src/stores/](src/stores/))

| Store | Holds | Notes |
|---|---|---|
| [`EntityStore`](src/stores/entity_store.py) | `entities.parquet` + food/chemical LUTs | `_curr_eid` for new ID allocation; JSON columns (synonyms, external_ids, attributes) deserialized on load. |
| [`EntityRegistry`](src/stores/entity_registry.py) | `entity_registry.parquet` | Persistent `(source, native_id) → fa_id` map, 1:N via aliases. |
| [`TripletStore`](src/stores/triplet_store.py) | `triplets.parquet` | Composite-key indexed; merges attestation lists on duplicate keys. |
| [`EvidenceStore`](src/stores/evidence_store.py) | `evidence.parquet` | Source-anchored evidence rows; `reference` is JSON. |
| [`AttestationStore`](src/stores/attestation_store.py) | `attestations.parquet` (+ `attestations_ambiguous.parquet`) | Per-claim provenance: raw names, concentration, candidate IDs. |
| [`schema.py`](src/stores/schema.py) | — | Derives column lists from Pydantic model fields (respecting aliases) plus path constants. |
| [`registry_seeder.py`](src/stores/registry_seeder.py) | — | Bootstraps registry from a previous-KG TSV. |
| [`registry_diff.py`](src/stores/registry_diff.py) | — | Diffs two registries for the release report. |

The umbrella [`KnowledgeGraph`](src/pipeline/knowledge_graph.py) wires all
five stores together with ordered `_load` / `save` (FK order matters:
evidence → attestations → triplets → entities) and exposes `print_stats()`
for memory profiling.

---

## 10. Models ([src/models/](src/models/))

| Model | Role |
|---|---|
| [`Entity`](src/models/entity.py) + `FoodEntity` / `ChemicalEntity` / `DiseaseEntity` | `foodatlas_id`, `entity_type`, `common_name`, `scientific_name`, `synonyms[]`, `external_ids{}`, `attributes{}`. |
| [`Triplet`](src/models/triplet.py) | `head_id`, `relationship_id`, `tail_id`, `source`, `attestation_ids[]`. |
| [`Evidence`](src/models/evidence.py) | `evidence_id`, `source_type`, `reference` (JSON). |
| [`Attestation`](src/models/attestation.py) | `attestation_id`, `evidence_id`, `source`, raw names, concentration value/unit (raw + normalized), `food_part`, `food_processing`, `filter_score`, validation flags, `head_candidates[]`, `tail_candidates[]`. |
| [`RelationshipType`](src/models/relationship.py) | StrEnum with the five `r1`–`r5` codes. |
| [`AttributeKey`](src/models/attributes.py) | Canonical keys for the `attributes` JSON column. |
| [`SourceManifest`](src/models/ingest.py) | Ingest output metadata. |
| [`KGCSettings`](src/models/settings.py) | Pydantic `BaseSettings` (env prefix `KGC_`). |
| [`version.py`](src/models/version.py) | KG schema version string. |

---

## 11. Config & corrections ([src/config/](src/config/))

- [`defaults.json`](src/config/defaults.json) — base paths (`kg_dir`,
  `data_dir`, `output_dir`, `cache_dir`, `ie_raw_dir`).
- [`corrections.yaml`](src/config/corrections.yaml) + [`corrections.py`](src/config/corrections.py)
  — overlay-pattern manual fixes:
  - `chebi`: `drop_nodes`, `rename_nodes`.
  - `cdno`: `remap_xrefs`, `disambiguate_fdc`.
  - `fdc`: food / nutrient overrides.
  - `chebi_lut`: synonym blacklist to drop from the LUT.
  - `ontology_roots`: DFS anchors (`foodon_is_food`, `foodon_is_organism`,
    `chebi_molecular_entity`, `cdno_keep_subtrees`).
  - These are **not** mutated into the base KG; downstream code branches on
    them at read time (i.e. an overlay), preserving raw ingest output.
- [`foodatlas_classifications.yaml`](src/config/foodatlas_classifications.yaml)
  — curated category → ChEBI/FoodOn anchor map consumed by the enrichment
  classifiers.

---

## 12. Utilities ([src/utils/](src/utils/))

- [`orphans.py`](src/utils/orphans.py) — `find_orphans(entities, triplets)`
  identifies entities not referenced by any triplet; `write_orphans_jsonl`
  drops them into `diagnostics/kgc_orphans.jsonl`.
- [`unclassified.py`](src/utils/unclassified.py) — finds foods/chemicals with
  no IS_A parent, sorted by attestation count, written to
  `diagnostics/kgc_unclassified.jsonl`.
- [`timing.py`](src/utils/timing.py) — `log_duration(label, logger)` context
  manager.
- [`json_io.py`](src/utils/json_io.py) — `read_json` / `write_json` helpers.
- [`constants.py`](src/utils/constants.py) — pipeline-wide string constants.

---

## 13. Reporting ([src/pipeline/report/](src/pipeline/report/))

`run_diff(old_kg, kg_dir)` ([runner.py](src/pipeline/report/runner.py))
compares the current KG against `data/PreviousFAKG/v3.3/`:

- `compare_entities` — type counts, new / removed IDs, orphan deltas.
- `compare_triplets` — per-relationship counts, new / removed keys.
- `compare_entity_details` — name / type changes for stable IDs.
- `compare_sources` — per-source attestation / evidence counts.

Old-KG loading lives in [load_old.py](src/pipeline/report/load_old.py)
(legacy TSV parsing). Output rendering:

- `format_changelog()` ([format.py](src/pipeline/report/format.py)) — markdown
  release notes (the file shipped as `CHANGELOG.md` inside each bundle).
- `format_report()` — plaintext operator report.

---

## 14. Output layout

```
outputs/
├── ingest/<source_id>/
│   ├── <source_id>_nodes.parquet
│   ├── <source_id>_edges.parquet
│   ├── <source_id>_xrefs.parquet
│   └── <source_id>_manifest.json
└── kg/
    ├── entities.parquet
    ├── entity_registry.parquet
    ├── triplets.parquet
    ├── relationships.parquet
    ├── evidence.parquet
    ├── attestations.parquet
    ├── attestations_ambiguous.parquet
    ├── intermediate/
    │   ├── lookup_table_food.json
    │   └── lookup_table_chemical.json
    ├── checkpoints/<stage>/...           # full snapshot at stage boundary
    └── diagnostics/
        ├── kgc_orphans.jsonl
        ├── kgc_unclassified.jsonl
        ├── ie_unresolved.jsonl
        ├── ie_parse_errors.tsv
        └── ie_conc_unconverted.tsv
```

---

## 15. Publishing & sync ([scripts/](scripts/))

- [`_lib.sh`](scripts/_lib.sh) — resolves the KGC bucket from env / config and
  reads the `LATEST` pointer for a given prefix.
- [`sync-data-to-s3.sh`](scripts/sync-data-to-s3.sh) — uploads `data/` to
  `s3://<bucket>/data/<UTC-ts>/`, refreshes `data/LATEST`. Run when source
  ontologies refresh (≈ quarterly).
- [`sync-outputs-to-s3.sh`](scripts/sync-outputs-to-s3.sh) — uploads
  `outputs/` to `s3://<bucket>/outputs/<UTC-ts>/`, refreshes `outputs/LATEST`.
  Run after every successful KGC build.
- [`pull-data-from-s3.sh`](scripts/pull-data-from-s3.sh) — pulls
  `data/LATEST` into local `data/`.
- [`pull-from-s3.sh`](scripts/pull-from-s3.sh) — pulls the previous KG into
  `data/PreviousFAKG/<version>/` (baseline for registry seeding and the diff
  report).
- [`publish-bundle.sh`](scripts/publish-bundle.sh) — copies the KG plus
  `CHANGELOG.md` to the public downloads bucket, zips it, and refreshes
  `bundles/index.json`.

Every sync writes to an immutable timestamped path; old versions are never
deleted, so rollback is always possible.

---

## 16. Key design rules & gotchas

1. **Stable entity IDs.** The registry seeds from the previous KG; existing
   `(source, native_id)` pairs keep their `foodatlas_id`. New entities mint
   `e{next_eid}` from the registry's monotonically-increasing counter.
2. **Ambiguity preserved, not resolved.** LUTs return all matching IDs, and
   `explode_external_ids` / `ambiguity.py` propagate candidate lists into
   attestations so reviewers can disambiguate later. Ambiguous attestations
   live in a separate parquet so they never silently inflate aggregate counts.
3. **Two xref styles.** CDNO/FDC/DMD xrefs are registered (1:1, registry-backed);
   PubChem/MeSH xrefs are written only to `external_ids` because the mappings
   are legitimately many-to-many.
4. **Corrections are overlays.** `corrections.yaml` never mutates ingest
   output; downstream readers branch on its contents (e.g. ChEBI `drop_nodes`
   removes synonyms during LUT build, FDC overrides apply during xref linking).
5. **Checkpoints between every stage.** Each stage writes a full snapshot
   under `checkpoints/<stage_name>/`. The enrichment stage explicitly reloads
   from the `ie` checkpoint, which doubles as the rerun entry point if
   enrichment configuration changes.
6. **Schema is model-derived.** `src/stores/schema.py` builds column lists
   from Pydantic `model_fields`; adding a field to a model automatically
   propagates to the parquet schema (subject to alias rules).
7. **JSON in parquet.** `synonyms`, `external_ids`, `attributes`, and
   `reference` are JSON-encoded strings in parquet; stores transparently
   serialize on save and deserialize on load.
8. **Progress backpressure.** `IngestRunner` throttles its in-process progress
   callback to every ~500 ticks to avoid flooding the multiprocessing queue.
9. **Deprecated paths.** `data/Lit2KG/` is no longer read; the `lit2kg:*`
   source tag in [`src/pipeline/ie/loader.py`](src/pipeline/ie/loader.py) is a
   provenance label only — literature inputs now originate in
   [`backend/ie/`](../ie/).
