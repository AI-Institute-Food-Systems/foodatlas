# KGC Stories

All stories for the KGC code integration (Part 1) and data versioning (Part 2) from `docs/backend-integration-plan.md`.

---

## Part 1: KGC Code Integration

Restructure `FoodAtlas-KGv2/` into `backend/kgc/` as clean, maintainable modules.

### Story 1.1: Foundation — Config, Models, Utils

**Where**: `backend/kgc/src/`

Set up the project skeleton: `pyproject.toml`, `config/`, `models/`, `utils/`, `preprocessing/constants/`.

- [x] `pyproject.toml` with all dependencies; `uv sync` succeeds
- [x] `config/defaults.json` with default paths and empty API key placeholders
- [x] `models/settings.py` — `KGCSettings` with pydantic-settings, env prefix `KGC_`, loads defaults from `config/defaults.json`
- [x] `models/entity.py` — `Entity`, `FoodEntity`, `ChemicalEntity` with `ConfigDict(populate_by_name=True)` and `Field(alias=...)` for TSV-compatible serialization
- [x] `models/triplet.py` — `Triplet` model
- [x] `models/metadata.py` — `MetadataContains` model with aliased raw fields
- [x] `models/relationship.py` — `Relationship` model, `RelationshipType` enum (r1-r5)
- [x] `models/version.py` — `KGVersion` model
- [x] `utils/constants.py`, `utils/merge_sets.py` ported
- [x] `preprocessing/constants/` ported (greek_letters, punctuations, units)
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for settings loading, model validation, alias serialization

### Story 1.2: Core Data Model — Stores, Entity Discovery

**Where**: `backend/kgc/src/stores/`, `backend/kgc/src/discovery/`

**Depends on**: 1.1

Port runtime containers (EntityStore, TripletStore, MetadataContainsStore) and entity discovery logic.

- [x] `stores/schema.py` — column definitions derived from Pydantic models, file name constants
- [x] `stores/entity_store.py` — dict-based dispatch for LUT selection (replaces `eval()`), JSON LUT I/O
- [x] `stores/triplet_store.py` — `_make_key()`, `_insert_or_merge()`, `_key_to_metadata` dedup
- [x] `stores/metadata_store.py` — `_nan_to_empty()`, internal `_records` DataFrame
- [x] `discovery/food.py`, `discovery/chemical.py` — entity creation using model constructors
- [x] `discovery/query.py` — query stubs (implemented in 1.3)
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for entity creation, LUT ops, triplet dedup, metadata, round-trip save/load

### Story 1.3: Query Layer — NCBI and PubChem APIs

**Where**: `backend/kgc/src/discovery/`

**Depends on**: 1.1, 1.2

- [x] `discovery/query.py` — `query_ncbi_taxonomy()` and `query_pubchem_compound()` with settings injection (no module-level side effects)
- [x] `discovery/cache.py` — shared caching with per-batch incremental save, configurable `cache_dir`
- [x] PubChem: `input()` removed; `--pubchem-mapping-file` CLI option, error log if missing
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for cache hit/miss, query signatures, error handling

### Story 1.4: Preprocessing — Name and Concentration Standardization

**Where**: `backend/kgc/src/preprocessing/`

**Depends on**: 1.1

- [x] `preprocessing/chemical_name.py` — Greek letter normalization, punctuation standardization
- [x] `preprocessing/chemical_conc.py` — orchestrator (calls parser + converter)
- [x] `preprocessing/conc_parser.py` — regex patterns + `parse_conc_string`
- [x] `preprocessing/conc_converter.py` — unit conversion logic
- [x] `preprocessing/food_part.py` — food part standardization
- [x] Original 305-line file split so each new file is under 300 lines
- [x] All files pass ruff, mypy
- [x] Tests for chemical name, concentration parsing, unit conversion, food part, edge cases

### Story 1.5: KG Orchestrator — KnowledgeGraph Class

**Where**: `backend/kgc/src/constructor/`

**Depends on**: 1.2, 1.3, 1.4

- [x] `constructor/knowledge_graph.py` — accepts `KGCSettings`, dict dispatch (no `eval()`), no `nonlocal self`
- [x] `constructor/disambiguation.py` — synonym disambiguation, placeholder entity creation/resolution
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for KG load, entity creation, triplet creation, disambiguation

### Story 1.6: Initialization — Seed KG from Ontologies

**Where**: `backend/kgc/src/integration/`

**Depends on**: 1.2, 1.3

- [x] `integration/scaffold.py` — create empty files using `KGCSettings.kg_dir` and column constants from `stores/schema.py`
- [x] `integration/entities/food/init_entities.py`, `loaders.py` — food entity seeding from FoodOn and FDC
- [x] `integration/entities/chemical/init_entities.py`, `loaders.py` — chemical entity seeding from ChEBI, CDNO, FDC
- [x] `integration/ontologies/` — processors for foodon, chebi, cdno, mesh, pubchem
- [x] `integration/ontologies/food.py`, `chemical.py` — ontology creation
- [x] All paths configurable via `KGCSettings`
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for scaffold, entity initialization from sample data

### Story 1.7: Postprocessing — Grouping, Common Names, Synonyms

**Where**: `backend/kgc/src/postprocessing/`

**Depends on**: 1.2, 1.6

- [x] `postprocessing/common_name.py` — most-frequent-mention selection
- [x] `postprocessing/synonyms_display.py` — plural removal (inflection), ChEBI/MeSH synonyms
- [x] `postprocessing/grouping/chemicals.py` — CDNO + ChEBI grouping
- [x] `postprocessing/grouping/foods.py` — FoodOn hierarchy traversal, `clean_groups()` heuristic
- [x] `postprocessing/grouping/mesh.py` — MeSH category mapping (standard pandas, no pandarallel)
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for common name selection, synonym display, grouping logic

### Story 1.8: Integration — FDC, CTD, FlavorDB Merging

**Where**: `backend/kgc/src/integration/`

**Depends on**: 1.2, 1.5

- [x] `integration/triplets/fdc.py` — FDC merge (no `global metadata_rows`, uses list accumulator)
- [x] `integration/triplets/ctd.py` — CTD disease triplet/metadata creation, PMID mapping
- [x] `integration/triplets/flavordb.py` — FlavorDB flavor triplet creation
- [x] `integration/entities/disease/` — disease entity init from CTD
- [x] `integration/entities/flavor/` — flavor entity init from FlavorDB, HSDB loader
- [x] All paths configurable via `KGCSettings`
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for FDC merge, CTD data loading, FlavorDB triplets

### Story 1.9: Pipeline Runner + CLI

**Where**: `backend/kgc/src/pipeline/`, `backend/kgc/main.py`

**Depends on**: 1.1–1.8

- [x] `pipeline/stages.py` — `PipelineStage` enum (7 stages: ONTOLOGY_PREP through MERGE_FLAVOR)
- [x] `pipeline/runner.py` — `PipelineRunner` class with `run()`, `run_stage()`, validation, `_write_version()`
- [x] `main.py` — Click CLI with `run` (repeatable `--stage`), `init`, `--config`, `--output-format`, `-v`
- [x] Numeric stage indices accepted (e.g., `--stage 0`)
- [x] All files pass ruff, mypy, under 300 lines
- [x] Tests for stage enum, runner execution, CLI argument parsing

### Story 1.10: Cleanup — Delete Legacy, Final Lint Pass

**Depends on**: 1.1–1.9

- [x] `FoodAtlas-KGv2/` deleted
- [x] `.gitignore` updated (outputs/, data/, config/local.json)
- [x] `ruff check` — zero errors
- [x] `mypy` — zero errors
- [x] All files under 300 lines
- [x] All 409 tests pass
- [x] `examples/` retained as regression fixture

---

## Part 2: Data Versioning

Implement the overlay/override data versioning strategy. Ordered by dependency.

### Story 2.1: Expand version and redirect models

**Where**: `backend/kgc/src/models/version.py`

**Depends on**: 1.1

Update `KGVersion` to match the plan's `version.json` schema and add missing models.

- [ ] Add fields to `KGVersion`: `created_at` (datetime), `parent_version` (str | None), `version_type` (Literal["pipeline", "hotfix"]), `stats` (dict), `manifest` (dict)
- [ ] Add `ChangeLogEntry` model: `entity_id`, `field`, `old_value`, `new_value`, `version`
- [ ] Add `EntityRedirect` model: `old_id`, `new_id` (str | None), `action` (Literal["merged", "retired"]), `reason` (str), `source` (Literal["pipeline", "hotfix"]), `created_at`
- [ ] Tests for serialization/deserialization of all three models

**Acceptance**: Models validate correctly, existing tests still pass.

### Story 2.2: Add `pipeline_version` to data models

**Where**: `backend/kgc/src/models/entity.py`, `triplet.py`, `metadata.py`

**Depends on**: 2.1

- [ ] Add `pipeline_version: str = ""` field to `Entity`, `Triplet`, `MetadataContains`, `MetadataDisease`, `MetadataFlavor`
- [ ] Update `stores/schema.py` column lists to include `pipeline_version`
- [ ] Update any store save/load code that would break from the new field
- [ ] Update test fixtures to include `pipeline_version`

**Acceptance**: All existing KGC tests pass with the new field present.

### Story 2.3: Version bumping logic in pipeline runner

**Where**: `backend/kgc/src/pipeline/runner.py`

**Depends on**: 2.1, 2.2

Replace the hardcoded `"0.1.0"` in `_write_version()` with real semantic versioning.

- [ ] On full pipeline run: read previous `version.json` (if exists), bump MINOR, write new version with `parent_version`, `version_type="pipeline"`, `created_at`
- [ ] Compute `stats`: count entities added/removed, triplets added/removed vs. previous run's output
- [ ] Stamp `pipeline_version` onto all entity/triplet/metadata records before saving
- [ ] Use the `KGVersion` model (from 2.1) to write `version.json`
- [ ] Tests: mock previous version file, assert correct bump and stats

**Acceptance**: `version.json` output matches plan schema. `pipeline_version` appears on all output records.

### Story 2.4: Entity merge/retire output

**Where**: `backend/kgc/src/stores/entity_store.py`, `backend/kgc/src/pipeline/runner.py`

**Depends on**: 2.1

- [ ] Add `retire_entity(old_id, new_id, action)` method to `EntityStore` that records the directive and removes `old_id` from the active entity set
- [ ] Accumulate retire/merge directives during pipeline run
- [ ] Write `retired.json` at pipeline save time using `EntityRedirect` model (from 2.1)
- [ ] Tests: merge two entities, verify retired output and entity removal

**Acceptance**: Pipeline outputs `retired.json` with correct format when merges occur.

### Story 2.5: Stable entity IDs across pipeline runs

**Where**: `backend/kgc/src/integration/entities/`, `backend/kgc/src/pipeline/runner.py`

**Depends on**: 2.1, 2.4

Make `KG_INIT` additive instead of from-scratch so that `foodatlas_id` values are stable across runs. This is a prerequisite for overrides and redirects to survive pipeline reruns.

**Problem**: Today `KG_INIT` calls `create_empty_files()` then assigns IDs sequentially (e1, e2, ...). If an updated ontology adds, removes, or reorders terms, every entity after the change point gets a different ID. Overrides and redirects targeting the old IDs become invalid.

**Solution**: On subsequent runs, load the previous KG output and reconcile against the new ontology data using `external_ids` as the stable key.

- [ ] Change `_run_kg_init` to detect whether a previous KG output exists in `kg_dir`
  - **First run** (no previous output): current from-scratch behavior
  - **Subsequent runs** (previous output exists): load previous KG, then reconcile
- [ ] Add `reconcile_entities(entity_store, new_ontology_df, source_key)` function that matches on `external_ids`:
  - **Unchanged**: entity exists with same external ID and same data — no-op
  - **Updated**: entity exists with same external ID but data changed (e.g., new synonyms) — update in place, keep `foodatlas_id`
  - **New**: external ID not found in existing entities — create with next available ID
  - **Removed**: existing entity's external ID not in new ontology — call `retire_entity()` (from 2.4) with `action="retired"`
  - **Merged**: two existing entities now map to the same ontology term — call `retire_entity()` with `action="merged"`, keep the lower ID
- [ ] Refactor `append_foods_from_foodon()`, `append_foods_from_fdc()`, `append_chemicals_from_chebi()`, `append_chemicals_from_cdno()`, `append_chemicals_from_fdc()` to call `reconcile_entities()` when previous data exists
- [ ] Rebuild LUTs and ontology files after reconciliation (existing code, no change needed)
- [ ] Record source ontology versions in `version.json` `manifest` field (e.g., `{"sources": {"foodon": "v2025-03-01.owl"}}`)
- [ ] Tests:
  - First run produces same output as today (backward compatible)
  - Subsequent run with unchanged ontology: all IDs preserved, no retirements
  - Subsequent run with added terms: new entities get new IDs, existing IDs unchanged
  - Subsequent run with removed terms: removed entities appear in `retired.json`
  - Subsequent run with merged terms: merged entity appears in `retired.json` with correct destination

**Acceptance**: Running the pipeline twice with the same ontology produces identical entity IDs. Running with an updated ontology preserves existing IDs, assigns new IDs only for new terms, and emits `retired.json` for removed/merged terms.

**Notes**:
- Entities discovered from literature (via `TRIPLET_EXPANSION`) are never re-created — they already exist in the previous KG and are loaded at the start. Their IDs are inherently stable.
- The reconciliation key per source: FoodOn → `external_ids.foodon`, ChEBI → `external_ids.chebi`, FDC → `external_ids.fdc`, NCBI → `external_ids.ncbi_taxon_id`, PubChem → `external_ids.pubchem_cid`.
- Triplets and metadata reference entities by `foodatlas_id`. Since IDs are stable, no triplet/metadata fixup is needed — only entities that are removed/merged require `retired.json` entries and triplet reassignment.

### Story 2.6: Project scaffolding for `backend/db/`

**Where**: `backend/db/`

- [ ] Add dependencies to `pyproject.toml`: `sqlalchemy>=2.0`, `psycopg[binary]>=3.1`, `alembic>=1.13`, `pydantic-settings>=2.2`
- [ ] Create `src/models/`, `src/loader/`, `src/queries/`, `migrations/` directories
- [ ] Create `src/settings.py` with `DBSettings` (pydantic-settings, env prefix `DB_`): `database_url`, `echo_sql`
- [ ] Create `src/session.py` with engine/session factory
- [ ] Tests: settings load from env, session factory creates engine

**Acceptance**: `cd backend/db && uv sync && uv run pytest` passes.

### Story 2.7: SQLAlchemy models for base tables

**Where**: `backend/db/src/models/`

**Depends on**: 2.6

Define ORM models matching the plan's base table schemas.

- [ ] `base_entities.py`: `BaseEntity` — columns: `foodatlas_id` (PK), `entity_type`, `common_name`, `scientific_name`, `synonyms` (ARRAY), `external_ids` (JSONB), `synonyms_display` (ARRAY), `pipeline_version`
- [ ] `base_triplets.py`: `BaseTriplet` — columns: `foodatlas_id` (PK), `head_id`, `relationship_id`, `tail_id`, `metadata_ids` (ARRAY), `pipeline_version`; unique constraint on `(head_id, relationship_id, tail_id)`
- [ ] `base_metadata.py`: `BaseMetadataContains` — all columns from plan section 2.3
- [ ] `relationships.py`: `Relationship` — `relationship_id` (PK), `name` (unique)
- [ ] Tests: models can create tables against an in-memory SQLite (or test PG)

**Acceptance**: All base table models match plan section 2.3.

### Story 2.8: SQLAlchemy models for overrides, redirects, versions

**Where**: `backend/db/src/models/`

**Depends on**: 2.6

- [ ] `overrides.py`: `Override` — `id` (serial PK), `target_table`, `target_id`, `field`, `value` (JSONB), `reason`, `author`, `applied_at`, `superseded` (bool, default false)
- [ ] `entity_redirects.py`: `EntityRedirect` — `old_id` (PK), `new_id` (nullable), `action`, `reason`, `source`, `created_at`
- [ ] `kg_versions.py`: `KGVersionRecord` — `version` (PK), `created_at`, `parent_version`, `version_type`, `manifest` (JSONB)
- [ ] Tests: models create tables, basic insert/query works

**Acceptance**: All support table models match plan section 2.3.

### Story 2.9: SQL migrations

**Where**: `backend/db/migrations/`

**Depends on**: 2.7, 2.8

- [ ] `001_base_tables.sql`: CREATE for `base_entities`, `base_triplets`, `base_metadata_contains`, `relationships`
- [ ] `002_overrides_and_redirects.sql`: CREATE for `overrides`, `entity_redirects`, `kg_versions`
- [ ] `003_live_views.sql`: `apply_overrides()` function + `live_entities`, `live_triplets`, `live_metadata_contains` views (as in plan section 2.3)
- [ ] Decide: raw SQL files vs Alembic migrations (either works, but should be runnable via a single command)

**Acceptance**: Migrations run clean against a fresh PG database. Views return correct merged data when tested manually.

### Story 2.10: Pipeline loader (TRUNCATE + COPY)

**Where**: `backend/db/src/loader/pipeline.py`

**Depends on**: 2.5, 2.9

- [ ] Read KGC output files (entities, triplets, metadata) from `kg_dir`
- [ ] Within a transaction: TRUNCATE `base_entities`, `base_triplets`, `base_metadata_contains`; COPY/bulk-insert from pipeline output
- [ ] Insert into `kg_versions` with `version_type='pipeline'`
- [ ] Tests: load fixture data, verify base table contents

**Acceptance**: Loader populates base tables from pipeline output. `live_*` views reflect the data.

### Story 2.11: Redirect loader

**Where**: `backend/db/src/loader/redirects.py`

**Depends on**: 2.10

- [ ] Read `retired.json` from KGC output
- [ ] Insert/upsert into `entity_redirects` with `source='pipeline'`
- [ ] Tests: load fixture redirects, verify `live_entities` excludes redirected IDs

**Acceptance**: Merged/retired entities appear in `entity_redirects` and are filtered from live views.

### Story 2.12: Override cleanup after pipeline load

**Where**: `backend/db/src/loader/cleanup.py`

**Depends on**: 2.10

- [ ] After pipeline load: compare active overrides against new base values
- [ ] Mark `superseded = TRUE` where the base value now matches the override value
- [ ] Tests: create override, load pipeline data that matches, verify superseded

**Acceptance**: Overrides are automatically superseded when the pipeline catches up.

### Story 2.13: Apply overrides (hotfix system)

**Where**: `backend/db/src/loader/overrides.py`

**Depends on**: 2.10

- [ ] Function to insert a new override: `apply_override(target_table, target_id, field, value, reason, author)`
- [ ] Validate that `target_id` exists in the corresponding base table
- [ ] Auto-create a PATCH version bump in `kg_versions`
- [ ] Tests: apply override, verify it shows in `live_*` view

**Acceptance**: Hotfix is two inserts (override + version). Live view reflects the change immediately.

### Story 2.14: Revert overrides

**Where**: `backend/db/src/loader/overrides.py`

**Depends on**: 2.13

- [ ] Function to supersede a specific override by ID
- [ ] Tests: apply then revert, verify live view returns to base value

**Acceptance**: Overrides can be manually reverted.

### Story 2.15: Entity merge via hotfix

**Where**: `backend/db/src/loader/redirects.py`

**Depends on**: 2.10

- [ ] Function to merge two entities as a hotfix: insert into `entity_redirects` with `source='hotfix'`, create PATCH version
- [ ] Tests: merge entity, verify old ID excluded from live view, redirect record exists

**Acceptance**: Entities can be merged outside of pipeline runs.

### Story 2.16: Entity queries

**Where**: `backend/db/src/queries/entities.py`

**Depends on**: 2.13

- [ ] `get_entity(entity_id)` — read from `live_entities`; if ID is in `entity_redirects`, return redirect response shape
- [ ] `list_entities(entity_type, page, page_size)` — paginated read from `live_entities`
- [ ] `get_entity_history(entity_id)` — overrides timeline from `overrides` table
- [ ] Tests against fixture data

**Acceptance**: Query functions return correct shapes. Redirect responses include `merged_into` and `_links.canonical`.

### Story 2.17: Triplet and metadata queries

**Where**: `backend/db/src/queries/triplets.py`, `metadata.py`

**Depends on**: 2.10

- [ ] `get_triplets_for_entity(entity_id)` — from `live_triplets`
- [ ] `get_metadata(metadata_id)` — from `live_metadata_contains`
- [ ] Tests against fixture data

**Acceptance**: Query functions return correct data from live views.

### Story 2.18: Version queries

**Where**: `backend/db/src/queries/versions.py`

**Depends on**: 2.10

- [ ] `list_versions()` — all rows from `kg_versions`
- [ ] `get_changelog(version)` — structured diff from `kg_versions.manifest` + related overrides
- [ ] Tests against fixture data

**Acceptance**: Version queries return correct shapes and data.

---

## Dependency graph

```
Part 1 (all done):
1.1 -> 1.2 -> 1.3
  \      \-> 1.5 -> 1.8
   \-> 1.4 /    \
    \-> 1.6 -> 1.7
                 \
          1.1-1.8 -> 1.9 -> 1.10

Part 2:
2.1 -> 2.2 -> 2.3
 \-> 2.4 -> 2.5 ----\
                      \
2.6 -> 2.7 -> 2.9 -> 2.10 -> 2.11
 \-> 2.8 /            \-> 2.12
                       \-> 2.13 -> 2.14
                       \-> 2.15
                       \-> 2.16, 2.17, 2.18
```

- Stories 2.1–2.5 (KGC model + ID stability) and 2.6–2.9 (DB schema) can proceed in parallel
- Story 2.5 (ID stability) is a prerequisite for the DB loader (2.10) — without stable IDs, overrides/redirects are meaningless
- Stories 2.10+ (loader, overrides, queries) require both KGC output format and DB tables
- Stories 2.16–2.18 (read queries) can proceed in parallel once 2.10 is done

---

## Out of scope (for now)

- API endpoints (lives in `backend/api/`, depends on 2.16–2.18 queries)
- Frontend banners/badges for merged/overridden entities
- Auto-generated release changelogs
- Alembic setup for ongoing schema evolution (just raw migrations for now)
- Performance tuning of `live_*` views (materialized views, indexes)
