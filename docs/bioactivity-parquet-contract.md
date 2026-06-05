# Bioactivity Parquet Contract

Status: draft, 2026-06-05

This is the parquet shape the KGC pipeline must emit for bioactivity data so the DB loader (`backend/db`) can pick it up without a special-case ingest path. The DB/API/Frontend work is being built against fixtures matching this contract in parallel with the KGC ingest code.

## Files

The DB loader reads from `--parquet-dir` (default `backend/kgc/outputs/kg/`). Bioactivity adds **one new file** and **extends three existing ones**:

| File | Status | Change |
|---|---|---|
| `entities.parquet` | existing | new rows with `entity_type="bioactivity"` |
| `relationships.parquet` | existing | three new rows: `r5`, `r6`, `r7` |
| `triplets.parquet` | existing | new rows for r5/r6/r7 |
| `bioactivity_attestations.parquet` | **new** | full schema below |

`evidence.parquet`, `attestations.parquet`, and `trust_signals.parquet` are unchanged.

## Relationships

Add to `relationships.parquet`:

| relationship_id | relationship_type | description |
|---|---|---|
| `r5` | `MEASURED` | Chemical measured against Bioactivity (assay) |
| `r6` | `EXHIBITS` | Food exhibits Bioactivity (direct or inherited) |
| `r7` | `ASSOCIATED_WITH` | Bioactivity associated with Disease |

These must also be added to `RelationshipType` in `backend/kgc/src/models/relationship.py` and seeded via the scaffold step.

## Entities

Bioactivity entities follow the existing `Entity` model. The `entity_type` literal in `backend/kgc/src/models/entity.py` must be widened to include `"bioactivity"`.

For each bioactivity row in the source CSV:

- `foodatlas_id` — `e<N>` minted via `EntityRegistry.next_eid`, **lowercase**, same namespace as food/chemical/disease
- `entity_type` — `"bioactivity"`
- `common_name` — bioactivity label (e.g. `"anti-inflammatory"`)
- `scientific_name` — empty string
- `synonyms` — list, may be empty
- `external_ids` — dict, JSON-encoded in parquet. Keys:
  - `bioactivity_native_id` — the source `E*` id (e.g. `"E300001"`); **this is where `E300001` lives, not in `foodatlas_id`**
  - `chebi`, `mesh`, `bao`, `mondo`, `go` — optional, present when the source CSV supplies them
- `attributes` — dict with `description` populated from the source CSV

No parent hierarchy: `parent_label_ids` is intentionally not represented. Pranav (2026-06-05): each MeSH is treated independently.

Register `("bioactivity", "E300001") → e<N>` in the entity registry so the namespace stays uniform.

## Triplets

Standard `Triplet` shape (`head_id`, `relationship_id`, `tail_id`, `source`, `attestation_ids`). Three new triplet kinds:

### r5 — Chemical measured Bioactivity

- `head_id` — chemical `foodatlas_id` (resolved from ChEBI in `chemical_bioactivity_triplets.csv` using the existing `explode_external_ids(..., "chebi")` pattern from `backend/kgc/src/pipeline/triplets/food_chemical/fdc.py`)
- `relationship_id` — `"r5"`
- `tail_id` — bioactivity `foodatlas_id`
- `source` — `"bioactivity"`
- `attestation_ids` — one `ba_*` id per BAM row backing this (chemical, bioactivity) pair

### r6 — Food exhibits Bioactivity

- `head_id` — food `foodatlas_id` (resolved from FoodOn)
- `relationship_id` — `"r6"`
- `tail_id` — bioactivity `foodatlas_id`
- `source` — `"bioactivity"`
- `attestation_ids` — one or more `ba_*` ids

**Direct vs inherited** is encoded on the attestations, not the triplet. One r6 triplet per `(food, bioactivity)` aggregates both direct and inherited attestations; the DB materializer splits them by `exhibit_type` into separate MV rows.

### r7 — Bioactivity associated with Disease

- `head_id` — bioactivity `foodatlas_id`
- `relationship_id` — `"r7"`
- `tail_id` — disease `foodatlas_id` (resolved from MeSH)
- `source` — `"bioactivity"`
- `attestation_ids` — one `ba_*` id per BDM row

## Inherited r6 edges

KGC produces the inherited attestations by joining r1 (food contains chemical) × r5 (chemical measured bioactivity). The DB does **not** redo this join.

For each `(food, chemical, bioactivity)` triple where the food contains the chemical and the chemical was measured against the bioactivity:

- emit one `ba_*` attestation row with `exhibit_type="inherited"`, `via_chemical_id=<chemical foodatlas_id>`, `derived_from_attestation_id=<originating r5 ba_*>`
- append its id to the `attestation_ids` of the corresponding r6 triplet
- if a direct r6 already exists for this `(food, bioactivity)`, append to it; otherwise create a new r6 triplet

Carry both potency and Hill-curve fields through from the originating r5 attestation so the DB materializer can compute `Efficacy_Pred` without re-joining.

Inherited-edge safety cap: ship a soft guard of 5000 inherited edges per food (configurable via `KGCSettings`). With ~50 bioactivities total and the expected chemicals/food, this should never trigger.

## `bioactivity_attestations.parquet`

New file alongside the existing `attestations.parquet`. **Separate from `attestations.parquet`** because of the ~12 extra columns specific to assay metadata (potency, Hill coefficients, target ids) that are irrelevant to FDC/literature attestations.

`attestation_id` is content-addressed and prefixed `ba_` (the existing namespace is `at_`). Use a `BioactivityAttestationStore` mirroring `AttestationStore`.

### Schema

JSON-encoded columns are written as JSON strings in parquet (same convention as `external_ids` in `entities.parquet`).

| column | type | nullable | applies to | notes |
|---|---|---|---|---|
| `attestation_id` | string | no | all | `ba_<hash>` |
| `evidence_id` | string | no | all | FK → `evidence.parquet` |
| `bioactivity_metadata_id` | string | no | all | `BAM*` for r5/r6, `BDM*` for r7 |
| `source` | string | no | all | e.g. `"bioactivity"` |
| `head_name_raw` | string | yes | all | raw label from the source CSV |
| `tail_name_raw` | string | yes | all | raw label from the source CSV |
| `head_candidates` | list[string] (JSON) | no | all | resolved foodatlas_ids (`len==1` pristine) |
| `tail_candidates` | list[string] (JSON) | no | all | resolved foodatlas_ids (`len==1` pristine) |
| `validated` | bool | no | all | default `false` |
| `validated_correct` | bool | no | all | default `true` |
| `source_assay_id` | string | yes | r5, inherited r6 | e.g. PubChem AID |
| `target_id` | string (JSON list) | yes | r5, r6, r7 | UniProt ids; JSON list to allow multiples on r7 |
| `evidence_value_potency_value` | float | yes | r5, inherited r6 | |
| `evidence_value_potency_unit` | string | yes | r5, inherited r6 | e.g. `"uM"` |
| `evidence_value_efficacy_zeroactivity` | float | yes | r5, inherited r6 | Hill curve |
| `evidence_value_efficacy_infiniteactivity` | float | yes | r5, inherited r6 | Hill curve |
| `evidence_value_efficacy_logac50_value` | float | yes | r5, inherited r6 | Hill curve |
| `evidence_value_efficacy_hillslope` | float | yes | r5, inherited r6 | Hill curve |
| `evidence_source` | string | yes | all | DOI / PubChem AID / etc. |
| `evidence_type` | string | yes | all | e.g. `literature`, `assay`, `inferred` |
| `exhibit_type` | string | yes | r6 only | `"direct"` or `"inherited"` |
| `polarity` | string | yes | r7 only | `"improves"` / `"degrades"`; **NULL for v1** — current sample CSV emits only `associated_with`; column reserved for future delivery |
| `derived_from_attestation_id` | string | yes | inherited r6 only | the originating r5 `ba_*` |
| `via_chemical_id` | string | yes | inherited r6 only | the bridging chemical's `foodatlas_id` |

Columns not applicable to a given row are NULL.

## Source CSVs reference

For traceability — the KGC ingest adapter reads from `backend/kgc/raw/bioactivity/`:

- `bioactivity_entities.csv` — one row per bioactivity entity
- `bioactivity_metadata.csv` — `BAM*` rows with assay metadata (potency, Hill coefficients, source assay id)
- `chemical_bioactivity_triplets.csv` — `(ChEBI id, bioactivity E*, BAM ids list)`
- `food_bioactivity_triplets.csv` — `(FoodOn id, bioactivity E*, BAM ids list)`
- `disease_bioactivity_triplets.csv` — `(bioactivity E*, relationship list, MeSH id, BDM ids list)`
- `bioactivity_disease_metadata.csv` — `BDM*` rows with `target_ids` (UniProt list)

Heads up to KGC author: the column header `disease MeSH id` in `disease_bioactivity_triplets.csv` has an embedded space. Normalise to `disease_mesh_id` in your reader to avoid downstream parsing headaches.

## Verification

Once the KGC pipeline emits the four files above, the DB load must succeed end-to-end:

```
cd backend/kgc && uv run python main.py run-pipeline
cd backend/db && uv run python main.py load
psql -c "select count(*) from base_bioactivity_attestations; select * from mv_bioactivity_entities limit 5;"
```

Until then, both sides develop against hand-rolled fixture parquets matching this contract.

## Open items

- **Polarity on r7**: reserved column, NULL in v1. Revisit when Pranav adds improves/degrades.
- **`disease MeSH id` column rename**: KGC ingest must normalise to `disease_mesh_id`.
