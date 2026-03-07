# KGC Integration & Data Versioning Strategy

## Context

The `FoodAtlas-KGv2` repo was cloned into `backend/kgc/FoodAtlas-KGv2/`. It's a research-grade codebase with hardcoded paths, no type annotations, files exceeding 300 lines, `eval()` usage, interactive `input()` calls, and `global` variables. It needs to be restructured into `backend/kgc/` as multiple top-level packages and made maintainable/deployable. Additionally, we need a data versioning strategy for when the KG goes live — handling entity merges, metadata updates, backward compatibility, and change communication.

This plan has **two parts**: (1) code integration, (2) data versioning architecture.

### Confirmed Decisions

- **Migration approach**: Full rewrite into `backend/kgc/` as multiple top-level packages, delete `FoodAtlas-KGv2/` when done
- **Database**: PostgreSQL via AWS Serverless (DB code not yet migrated)
- **Versioning**: Overlay/override pattern — base tables (pipeline) + overrides table (hotfixes) + live views (merged read layer)
- **PR strategy**: Single large PR for the full KGC rewrite

---

## Part 1: KGC Code Integration

### 1.1 Target Module Structure

```
backend/kgc/
  pyproject.toml              # Updated with real dependencies
  main.py                     # Click CLI entry point
  config/
    defaults.json             # Default configuration values (paths, API keys, etc.)
  models/
    __init__.py
    settings.py               # Pydantic Settings (replaces all hardcoded paths, loads from config/)
    entity.py                 # Entity, FoodEntity, ChemicalEntity pydantic models
    triplet.py                # Triplet model
    metadata.py               # MetadataContains model
    relationship.py           # Relationship model (r1-r5)
    version.py                # KGVersion, ChangeLogEntry, EntityRedirect models
  pipeline/
    __init__.py
    runner.py                 # Orchestrator replacing shell scripts
    stages.py                 # Stage enum + registry
  kg/
    __init__.py
    knowledge_graph.py        # From _kg.py (cleaned up)
    disambiguation.py         # Extracted from _kg.py if needed for 300-line limit
    entities/
      __init__.py
      base.py                 # From _base.py
      food.py                 # From _food.py
      chemical.py             # From _chemical.py
    triplets.py               # From _triplets.py
    metadata.py               # From _metadata.py
  query/
    __init__.py
    ncbi.py                   # Split from _query.py
    pubchem.py                # Split from _query.py
    cache.py                  # Shared query caching
  preprocessing/
    __init__.py
    chemical_name.py
    chemical_conc.py          # Orchestrator (calls parser + converter)
    conc_parser.py            # Extracted: regex + parse logic
    conc_converter.py         # Extracted: unit conversion
    food_part.py
    constants/
      __init__.py
      greek_letters.py
      punctuations.py
      units.py
  initialization/
    __init__.py
    scaffold.py               # create_empty_files
    food/
      __init__.py
      init_entities.py
      init_onto.py
      loaders.py              # Combined _load_fdc + _load_foodon
    chemical/
      __init__.py
      init_entities.py        # Split if >300 lines
      init_onto.py
      loaders.py              # Combined _load_cdno/chebi/fdc/mesh/pubchem
  postprocessing/
    __init__.py
    synonyms_display.py
    common_name.py
    grouping/
      __init__.py
      chemicals.py
      foods.py
      mesh.py
  integration/
    __init__.py
    fdc.py
    ctd/
      __init__.py
      processing.py
      pmid_mapping.py
      merger.py
      factd_parser.py         # Split from create_factd_data.py (530 lines)
      factd_builder.py        # Split from create_factd_data.py
      utils/
        __init__.py
        data_loaders.py       # Split from data.py (643 lines)
        data_transforms.py    # Split from data.py
    flavordb/
      __init__.py
      food_flavors.py
      hsdb_loader.py
  data_processing/
    __init__.py
    cdno.py, chebi.py, foodon.py, mesh.py, pubchem.py
  utils/
    __init__.py
    constants.py
    merge_sets.py
  tests/
    conftest.py               # Shared fixtures
    test_knowledge_graph.py
    test_entities.py
    test_triplets.py
    test_metadata.py
    test_preprocessing.py
    test_pipeline.py
    fixtures/                 # Minimal TSV test data
```

### 1.2 Key Refactoring Decisions

| Issue | Fix |
|-------|-----|
| Hardcoded paths everywhere | `models/settings.py` with `pydantic-settings`, env vars prefixed `KGC_`; defaults in `config/defaults.json` |
| `eval(f"self.entities._lut_{entity_type}")` | Dict-based dispatch: `{"food": self._lut_food, ...}[entity_type]` |
| Interactive `input()` in PubChem flow | CLI option `--pubchem-mapping-file`; error if missing |
| `global metadata_rows` in merge_fdc | Return values / list accumulator pattern |
| Module-level `Entrez.email` read from file | Inject via `KGCSettings` |
| `pandarallel.initialize()` at module scope | Make optional with try/except fallback to `apply()` |
| `print()` statements | Replace with `logging` |
| 5 files exceeding 300 lines | Split as shown in structure above |
| Shell script pipeline | Python `PipelineRunner` class with Click CLI |

### 1.3 Configuration (`models/settings.py` + `config/defaults.json`)

```python
class KGCSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KGC_")
    kg_dir: Path = Path("outputs/kg")
    cache_dir: Path = Path("outputs/kg/_cache")
    data_dir: Path = Path("data")
    ncbi_email: str = ""
    ncbi_api_key: str = ""
    model_name: str = "gpt-3.5-ft"
    parallel_workers: int = 4
```

### 1.4 Dependencies (for `pyproject.toml`)

Core: `pandas`, `click`, `biopython`, `inflection`, `tqdm`, `pympler`, `pydantic-settings`, `owlready2`, `rdflib`, `lxml`, `openpyxl`, `numpy`
Optional: `pandarallel` (graceful fallback)

### 1.5 Migration Phases

1. **Foundation**: config, utils, preprocessing constants (no behavior change)
2. **Core data model**: entities, triplets, metadata classes (add types, use Path)
3. **Query layer**: split _query.py, remove module-level side effects
4. **Preprocessing**: split conc standardization into parser/converter
5. **KG orchestrator**: port _kg.py, fix eval/nonlocal patterns
6. **Init/postprocessing/integration**: split oversized files, port all modules
7. **Pipeline + CLI**: replace shell scripts with PipelineRunner
8. **Testing**: fixtures, 80% coverage target
9. **Cleanup**: delete FoodAtlas-KGv2/, update gitignore, run all linters

---

## Part 2: Data Versioning Strategy

### 2.1 Architecture Overview (Overlay/Override Pattern)

The database uses a three-layer pattern to cleanly separate pipeline data from manual corrections:

```
┌─────────────────────────────────────────────────┐
│  base_* tables     (pipeline writes here)        │
│  Full overwrite each pipeline run.               │
│  No hotfix logic. Pipeline stays simple.         │
├─────────────────────────────────────────────────┤
│  overrides table   (hotfixes write here)         │
│  One row per field correction.                   │
│  Never touched by pipeline.                      │
├─────────────────────────────────────────────────┤
│  live_* views      (API reads from here)         │
│  Merges base + overrides at read time.           │
│  Override wins when present.                     │
└─────────────────────────────────────────────────┘
```

### 2.2 KG Versioning Scheme

**Semantic versioning**: `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking schema changes (relationship type removed, entity type redefined)
- **MINOR**: Additive (new literature batch, new data source, new entities/triplets)
- **PATCH**: Corrections (hotfixes — entity merges, name fixes, metadata corrections)

Each pipeline run produces `version.json`:

```json
{
  "version": "2.3.0",
  "created_at": "2026-03-06T12:00:00Z",
  "parent_version": "2.2.0",
  "version_type": "pipeline",
  "stats": { "entities_added": 142, "entities_merged": 1, "triplets_added": 856 }
}
```

### 2.3 Database Schema (PostgreSQL on AWS Serverless)

#### Base Tables (pipeline output — full overwrite each run)

```sql
CREATE TABLE base_entities (
    foodatlas_id     VARCHAR(20) PRIMARY KEY,
    entity_type      VARCHAR(20) NOT NULL,
    common_name      TEXT NOT NULL,
    scientific_name  TEXT,
    synonyms         TEXT[] NOT NULL DEFAULT '{}',
    external_ids     JSONB NOT NULL DEFAULT '{}',
    synonyms_display TEXT[] DEFAULT '{}',
    pipeline_version VARCHAR(20) NOT NULL
);

CREATE TABLE base_triplets (
    foodatlas_id    VARCHAR(20) PRIMARY KEY,
    head_id         VARCHAR(20) NOT NULL,
    relationship_id VARCHAR(10) NOT NULL,
    tail_id         VARCHAR(20) NOT NULL,
    metadata_ids    VARCHAR(20)[] NOT NULL DEFAULT '{}',
    pipeline_version VARCHAR(20) NOT NULL,
    UNIQUE(head_id, relationship_id, tail_id)
);

CREATE TABLE base_metadata_contains (
    foodatlas_id          VARCHAR(20) PRIMARY KEY,
    conc_value            DOUBLE PRECISION,
    conc_unit             TEXT,
    food_part             TEXT,
    food_processing       TEXT,
    source                TEXT,
    reference             JSONB,
    entity_linking_method TEXT,
    quality_score         DOUBLE PRECISION,
    food_name_raw         TEXT,
    chemical_name_raw     TEXT,
    conc_raw              TEXT,
    food_part_raw         TEXT,
    pipeline_version      VARCHAR(20) NOT NULL
);

CREATE TABLE relationships (
    relationship_id VARCHAR(10) PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE
);
```

#### Overrides Table (hotfixes — never touched by pipeline)

```sql
CREATE TABLE overrides (
    id           SERIAL PRIMARY KEY,
    target_table VARCHAR(30) NOT NULL,   -- 'entities', 'triplets', 'metadata_contains'
    target_id    VARCHAR(20) NOT NULL,   -- foodatlas_id of the row
    field        TEXT NOT NULL,          -- which column to override
    value        JSONB NOT NULL,         -- the corrected value
    reason       TEXT NOT NULL,          -- why this was changed
    author       TEXT NOT NULL,          -- who made the fix
    applied_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    superseded   BOOLEAN NOT NULL DEFAULT FALSE  -- TRUE when pipeline catches up
);
```

#### Entity Redirects (merges and retirements)

```sql
CREATE TABLE entity_redirects (
    old_id     VARCHAR(20) PRIMARY KEY,
    new_id     VARCHAR(20),              -- NULL if permanently retired
    action     VARCHAR(20) NOT NULL,     -- 'merged', 'retired'
    reason     TEXT,
    source     VARCHAR(20) NOT NULL,     -- 'pipeline' or 'hotfix'
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

#### Version Tracking

```sql
CREATE TABLE kg_versions (
    version        VARCHAR(20) PRIMARY KEY,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    parent_version VARCHAR(20),
    version_type   VARCHAR(10) NOT NULL,  -- 'pipeline' or 'hotfix'
    manifest       JSONB NOT NULL
);
```

#### Live Views (what the API reads)

The API never reads base tables directly. It reads from views that merge base + overrides:

```sql
CREATE OR REPLACE FUNCTION apply_overrides(
    base_row JSONB,
    p_target_table TEXT,
    p_target_id TEXT
) RETURNS JSONB AS $$
    SELECT base_row || COALESCE(
        (SELECT jsonb_object_agg(o.field, o.value)
         FROM overrides o
         WHERE o.target_table = p_target_table
           AND o.target_id = p_target_id
           AND o.superseded = FALSE),
        '{}'::JSONB
    );
$$ LANGUAGE SQL STABLE;

CREATE VIEW live_entities AS
SELECT
    (apply_overrides(to_jsonb(e), 'entities', e.foodatlas_id))->>'foodatlas_id' AS foodatlas_id,
    (apply_overrides(to_jsonb(e), 'entities', e.foodatlas_id))->>'entity_type' AS entity_type,
    (apply_overrides(to_jsonb(e), 'entities', e.foodatlas_id))->>'common_name' AS common_name,
    -- ... other fields
    e.pipeline_version,
    EXISTS(
        SELECT 1 FROM overrides o
        WHERE o.target_table = 'entities'
          AND o.target_id = e.foodatlas_id
          AND o.superseded = FALSE
    ) AS has_overrides
FROM base_entities e
WHERE e.foodatlas_id NOT IN (SELECT old_id FROM entity_redirects);
```

### 2.4 Workflows

#### New Literature Batch (MINOR version bump)

```
New papers (PMC articles)
       |
       v
IE sub-project (backend/ie/) extracts raw metadata
       |
       v
KGC Pipeline:
  Stage 1: Metadata Processing
    - Parse raw extraction output
    - Standardize chemical names, concentrations
    - Exact-match entity linking against lookup tables
  Stage 2: KG Expansion
    - Matched entities -> new triplets
    - Unmatched names -> query NCBI/PubChem -> new entities
    - Deduplicate against existing triplets
  Stage 3: Postprocessing
    - Recompute entity groups, common names
    - Update synonym display lists
  Output: FULL set of TSVs + version.json
       |
       v
DB Loader:
  1. TRUNCATE base_entities, base_triplets, base_metadata_contains
  2. COPY full TSV output into base_* tables
  3. Process retired.tsv -> INSERT into entity_redirects
  4. Cleanup: supersede overrides where base now matches override value
  5. INSERT into kg_versions (version_type='pipeline')
       |
       v
live_* views automatically reflect new data + any active overrides
```

The pipeline always outputs the **full KG** (not just the diff). Each run takes the previous KG output as its starting point, adds new data, and outputs a complete snapshot:

```
Pipeline run N:   50,000 entities, 200,000 triplets
+ New literature:   500 papers -> 2,000 new triplets, 300 new entities
= Pipeline run N+1: 50,300 entities, 202,000 triplets  (full output)
```

#### Hotfix (PATCH version bump)

A hotfix is two inserts. No pipeline run needed.

```sql
-- Fix a typo
INSERT INTO overrides (target_table, target_id, field, value, reason, author)
VALUES ('entities', 'e123', 'common_name', '"caffeine"', 'Typo fix', 'fzli');

-- Record the version
INSERT INTO kg_versions (version, version_type, manifest)
VALUES ('2.5.1', 'hotfix', '{"changes": [{"entity_id": "e123", "field": "common_name"}]}');
```

The `live_entities` view immediately reflects the change.

#### How Hotfixes Survive Pipeline Runs

Hotfixes are never overwritten because they live in a separate table:

**Before pipeline run:**
- `base_entities.e123.common_name` = "caffein" (pipeline has typo)
- `overrides`: e123.common_name = "caffeine"
- `live_entities.e123.common_name` = **"caffeine"** (override wins)

**After pipeline run (pipeline still has typo):**
- `base_entities.e123.common_name` = "caffein" (reloaded, still wrong)
- `overrides`: still there, untouched
- `live_entities.e123.common_name` = **"caffeine"** (override still wins)

**After pipeline run (pipeline code was fixed):**
- `base_entities.e123.common_name` = "caffeine" (pipeline fixed it)
- Cleanup step: marks override as `superseded = TRUE` (values now match)
- `live_entities.e123.common_name` = **"caffeine"** (base is correct on its own)

#### Override Cleanup (automatic, runs after each pipeline load)

```sql
UPDATE overrides o
SET superseded = TRUE
FROM base_entities e
WHERE o.target_table = 'entities'
  AND o.target_id = e.foodatlas_id
  AND o.superseded = FALSE
  AND o.field = 'common_name'
  AND o.value #>> '{}' = e.common_name;
-- repeat for other fields/tables
```

#### Entity Merge Flow

When two entities (e.g., e456 and e123) are the same compound:

1. **Via pipeline**: KGC outputs merge directive in `retired.tsv`: `(e456, merged, e123)`. DB loader inserts into `entity_redirects` with `source='pipeline'`.
2. **Via hotfix**: Admin inserts directly into `entity_redirects` with `source='hotfix'`.
3. **In both cases**: `live_entities` view excludes `e456` (filtered by `entity_redirects`). API request for `e456` checks `entity_redirects` and returns a redirect response.

### 2.5 Version Summary

| Event | What Happens | Version Bump |
|-------|-------------|-------------|
| New literature batch | Full KGC pipeline -> TRUNCATE + COPY base tables | MINOR (2.4.0 -> 2.5.0) |
| New data source (e.g., CTD) | Full KGC pipeline with integration stage | MINOR |
| Fix a typo / wrong link | INSERT into overrides | PATCH (2.5.0 -> 2.5.1) |
| Merge two entities | INSERT into entity_redirects | PATCH |
| Change schema (new column) | Alembic migration | MAJOR |

### 2.6 Backward Compatibility (API Patterns)

**Versioned access**:

```
GET /v1/entities/{id}                  -> latest state (from live_* view)
GET /v1/entities/{id}/history          -> change timeline (from overrides + kg_versions)
GET /v1/versions                       -> all KG versions
GET /v1/versions/{v}/changelog         -> structured diff
```

**Merged entity response**:

```json
{
  "foodatlas_id": "e456",
  "status": "merged",
  "merged_into": "e123",
  "merged_in_version": "2.3.1",
  "_links": { "canonical": "/v1/entities/e123" }
}
```

**Active entity with absorbed entities**:

```json
{
  "foodatlas_id": "e123",
  "status": "active",
  "absorbed_entities": ["e456"],
  "has_overrides": true,
  "pipeline_version": "2.5.0"
}
```

### 2.7 Change Presentation

**For API consumers**: `pipeline_version` and `has_overrides` on every response; `/changelog` endpoint with structured diffs

**For frontend users**:

- Merged entities: banner "This entity was merged into [X] in version Y"
- Retired entities: banner "This entity was retired in version Y"
- Entity pages: "History" tab showing overrides timeline
- Absorbed entities shown as "Also known as: [names]"
- Manually corrected entities: subtle badge "manually verified"

**For releases**: Auto-generated changelog from `kg_versions` + `overrides` tables, categorized by type

### 2.8 DB Sub-project Structure

```
backend/db/
  models/            # SQLAlchemy models for all tables above
  loader/
    pipeline.py      # TRUNCATE + COPY base tables from KGC TSV output
    overrides.py     # Apply/revert hotfixes to overrides table
    cleanup.py       # Supersede overrides that match base values
    redirects.py     # Process retired.tsv into entity_redirects
  queries/           # Read queries for the API layer (against live_* views)
  migrations/
    001_base_tables.sql
    002_overrides_and_redirects.sql
    003_live_views.sql
```

---

## Verification

1. **KGC integration**: `cd backend/kgc && uv sync && uv run pytest` -- all tests pass with 80%+ coverage
2. **Linting**: `ruff check backend/kgc/` and `mypy backend/kgc/` pass clean
3. **Pipeline**: `cd backend/kgc && uv run python main.py run --stage 0_kg_init` runs without hardcoded path errors (with `KGC_DATA_DIR` and `KGC_KG_DIR` set)
4. **File sizes**: No code file in `backend/kgc/` exceeds 300 lines
5. **DB schema**: Migrations create base tables, overrides table, live views successfully
6. **Pipeline load**: Loader can TRUNCATE + COPY sample TSV fixtures into base tables
7. **Hotfix**: INSERT into overrides is reflected in live_* view immediately
8. **Override persistence**: Pipeline reload does not overwrite active overrides
9. **Override cleanup**: Supersede overrides when base catches up
10. **Entity redirects**: Merged entity IDs resolve correctly via API
