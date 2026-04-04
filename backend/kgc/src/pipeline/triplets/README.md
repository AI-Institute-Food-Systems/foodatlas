# Stage 2: Source Triplet Construction

## What this stage does

Builds source triplets (relationships) in the knowledge graph from ingest edges and resolved entities. Every triplet flows through the evidence → attestation → triplet data path, ensuring full provenance. Stage 3 (IE triplet expansion) adds additional triplets from information extraction.

## Inputs

- **Entities** from Stage 1 (entity resolution): `outputs/kg/entities.parquet`
- **Ingest edges** from Stage 0 (ingest): `outputs/ingest/{source}/{source}_edges.parquet`

## Outputs

All written to `outputs/kg/`:

| File | Description |
|------|-------------|
| `triplets.parquet` | All triplets keyed by `head_id_relationship_id_tail_id` |
| `evidence.parquet` | Evidence records (source URLs, references) |
| `attestations.parquet` | Attestation records linking evidence to triplets, with ambiguity candidates |
| `_ambiguity_report.json` | Summary of attestations where entity resolution was ambiguous |

## Workflow

Five builders run sequentially. Each follows the same steps:

1. Food ontology — FoodOn is_a edges → food is_a food
2. Chemical ontology — ChEBI is_a edges → chemical is_a chemical
3. Disease ontology — CTD disease is_a edges → disease is_a disease
4. Food-chemical composition — FDC contains edges → food CONTAINS chemical
5. Chemical-disease associations — CTD chemdis edges → chemical ↔ disease

### Steps within each builder

1. **Build lookup map** — scan `entities.parquet` to create a native ID → foodatlas entity ID map (e.g., MeSH ID → `[e42, e99]`).

2. **Resolve edges** — for each ingest edge, look up both head and tail native IDs in the map. Skip if either end has no match. If an ID maps to multiple entities, expand into all combinations (cartesian product).

3. **Create evidence** — for each resolved edge, create an evidence record with the source reference (e.g., FDC nutrient URL, CTD PubMed IDs, ontology source). `kg.evidence.create()` returns the `evidence_id`.

4. **Create attestation** — create an attestation record linking back to the evidence. Carries the raw names (`head_name_raw`, `tail_name_raw`) and `head_candidates`/`tail_candidates` (the full list of entity IDs the name resolved to — used for ambiguity tracking). `kg.attestations.create()` returns the `attestation_id`.

5. **Create triplet** — create the triplet `(head_id, relationship_id, tail_id)` indexed by `attestation_id`, so the triplet links back to its attestation. `kg.triplets.create()` stores it.

### Provenance chain

```
triplet.attestation_ids → attestation.evidence_id → evidence.reference
```

Any triplet can be traced back to its source data through this chain.

## Per-builder details

### Food ontology (`food_food.py`)

- **Source**: FoodOn `is_a` edges
- **Relationship**: `r2` (is_a)
- **ID map**: FoodOn URL → foodatlas_id
- **Ambiguity**: None (ontology entities are 1:1 by design)

### Chemical ontology (`chemical_chemical.py`)

- **Source**: ChEBI `is_a` edges
- **Relationship**: `r2` (is_a)
- **ID map**: ChEBI integer ID → foodatlas_id
- **Ambiguity**: None (ontology entities are 1:1 by design)

### Disease ontology (`disease_disease.py`)

- **Source**: CTD disease `is_a` edges
- **Relationship**: `r2` (is_a)
- **ID map**: CTD disease ID → foodatlas_id
- **Ambiguity**: None (ontology entities are 1:1 by design)

### Food-chemical (`food_chemical.py`)

- **Source**: FDC `contains` edges
- **Relationship**: `r1` (CONTAINS)
- **ID map**: FDC food ID → foodatlas_id, FDC nutrient ID → foodatlas_id
- **Ambiguity**: None (FDC IDs are 1:1 with entities)
- **Note**: Carries concentration value + unit from edge `raw_attrs`, evidence URL to FDC

### Chemical-disease (`chemical_disease.py`)

- **Source**: CTD `chemical_disease_association` edges with direct evidence
- **Relationship**: `r3` (POSITIVELY_CORRELATES_WITH) or `r4` (NEGATIVELY_CORRELATES_WITH)
- **ID map**: MeSH ID → chemical entities, CTD disease ID → disease entities
- **Ambiguity**: Possible — multiple chemicals can share the same MeSH ID (via PubChem bridge). `head_candidates` on the extraction has `len > 1`.

## Ambiguity tracking

When a native ID (e.g., MeSH ID) or synonym resolves to multiple entities, the pipeline creates one triplet per entity (cartesian product). The extraction record carries `head_candidates` and `tail_candidates` listing all entities the raw name resolved to:

- `len(candidates) == 1` → pristine, unambiguous
- `len(candidates) > 1` → ambiguous: the same evidence is shared across multiple triplets

The `_ambiguity_report.json` summarizes all ambiguous attestations. This is equivalent to a Wikipedia disambiguation page: it flags where the same evidence could apply to multiple entities, without silently inflating evidence counts.

## CLI

```bash
# Run triplets stage only
cd backend/kgc && uv run python main.py run --stage triplets

# Run full pipeline
uv run python main.py run
```

## Inspecting output

```bash
# View triplets
vd outputs/kg/triplets.parquet

# Check ambiguous attestations
vd outputs/kg/attestations.parquet

# Or with DuckDB
duckdb -c "
  SELECT head_name_raw, tail_name_raw, head_candidates, tail_candidates
  FROM 'outputs/kg/attestations.parquet'
  WHERE len(head_candidates) > 1 OR len(tail_candidates) > 1
  LIMIT 10
"
```
