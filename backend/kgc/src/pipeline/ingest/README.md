# Ingest (Phase 1)

## What this phase does

Ingest parses raw external data files into a standardized parquet format. No filtering, no corrections, no entity linking — just faithful parsing. The output is consumed by Phase 2 (construct), which builds the actual knowledge graph.

## Workflow

### No dependencies between sources

All 8 adapters are independent. No adapter reads another adapter's output. They can run in any order, or all in parallel:

```
data/FoodOn/     ──→  [FoodOnAdapter]   ──→  outputs/ingest/foodon/
data/ChEBI/      ──→  [ChEBIAdapter]    ──→  outputs/ingest/chebi/
data/CDNO/       ──→  [CDNOAdapter]     ──→  outputs/ingest/cdno/
data/CTD/        ──→  [CTDAdapter]      ──→  outputs/ingest/ctd/
data/MeSH/       ──→  [MeSHAdapter]     ──→  outputs/ingest/mesh/
data/PubChem/    ──→  [PubChemAdapter]  ──→  outputs/ingest/pubchem/
data/FlavorDB/   ──→  [FlavorDBAdapter] ──→  outputs/ingest/flavordb/
data/FDC/        ──→  [FDCAdapter]      ──→  outputs/ingest/fdc/
```

When you run `uv run python main.py run --stage ingest`, all 8 run in parallel via `ProcessPoolExecutor`. You can also run a single source with `--source foodon` — this does not invalidate or require any other source's output.

### Re-running is safe

Each adapter overwrites its own output directory. Re-running `--source foodon` replaces `outputs/ingest/foodon/` without touching any other source. This means:

- If a source's raw data is updated (e.g., new FoodOn release), re-run only that source.
- The other 7 sources keep their existing parquet files.
- Phase 2 always reads all 8 sources from `outputs/ingest/`, so it picks up the updated data on its next run.

### What each adapter does

Each adapter reads one set of raw files and produces up to 3 parquet files:

- **`{source}_nodes.parquet`** — Entities/concepts from the source, with native IDs, names, and synonyms.
- **`{source}_edges.parquet`** — Relationships between nodes (hierarchies, associations, composition).
- **`{source}_xrefs.parquet`** — Cross-references pointing to other databases.

Plus a **`{source}_manifest.json`** with row counts and timestamp.

Not every source produces all three. PubChem produces only xrefs (it's a bridge between databases, not an entity source). FlavorDB produces no edges.

### What ingest does NOT do

These are all deferred to Phase 2 (construct):

- **No domain filtering.** FoodOn outputs all 32K classes, not just the ~9K food products. ChEBI outputs all 205K compounds, not just molecular entities. CTD outputs all 9M chemdis edges, not just the 107K with direct evidence.
- **No manual corrections.** No ChEBI IDs are dropped or renamed. No CDNO xrefs are remapped. All corrections live in `corrections.yaml` and are applied in Phase 2.
- **No entity linking.** CDNO nodes are not linked to ChEBI entities. FDC foods are not linked to FoodOn entities. Cross-references exist in `_xrefs.parquet` but the actual resolution happens in Phase 2.
- **No ID assignment.** No `foodatlas_id` (e1, e2, ...) is generated. Nodes keep their native IDs.

## Parquet schemas

### Nodes

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | `str` | Adapter ID (e.g., `"foodon"`) |
| `native_id` | `str` | ID in source namespace (e.g., `"http://purl.obolibrary.org/obo/FOODON_00001234"`) |
| `name` | `str` | Primary label, lowercased |
| `synonyms` | `list[str]` | All synonyms |
| `synonym_types` | `list[str]` | Parallel list — type of each synonym (`"label"`, `"exact"`, `"broad"`, etc.) |
| `node_type` | `str` | Source-specific classification (`"class"`, `"compound"`, `"disease"`, `"food"`, `"nutrient"`, `"descriptor"`, `"supplemental"`, `"flavor_compound"`) |
| `raw_attrs` | `str` (JSON) | Additional attributes needed by Phase 2. Minimal — see raw_attrs section below. |

### Edges

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | `str` | Adapter ID |
| `head_native_id` | `str` | Source ID of the child/subject |
| `tail_native_id` | `str` | Source ID of the parent/object |
| `edge_type` | `str` | Relationship type (`"is_a"`, `"contains"`, `"chemical_disease_association"`, `"mapped_to"`, `"tree_parent"`) |
| `raw_attrs` | `str` (JSON) | Additional edge attributes needed by Phase 2 |

### Cross-references

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | `str` | Adapter ID |
| `native_id` | `str` | Source ID of the entity |
| `target_source` | `str` | Target database (`"chebi"`, `"foodon"`, `"fdc_nutrient"`, `"omim"`, `"do"`, `"pubchem_cid"`, `"mesh_term"`) |
| `target_id` | `str` | ID in target database |

## raw_attrs

Only attributes that Phase 2 actually consumes. Everything else is dropped.

| Source | File | raw_attrs | Phase 2 usage |
|--------|------|-----------|---------------|
| ChEBI | nodes | `{"star": 3}` | Common name priority — higher star = more trusted name |
| CTD | edges (chemdis) | `{"direct_evidence": "therapeutic"}` | Filter to direct-evidence-only associations |
| FDC | nodes (nutrient) | `{"unit_name": "mg"}` | Build concentration unit strings (e.g., `"mg/100g"`) |
| FDC | edges | `{"amount": 5.0}` | Nutrient concentration values |
| FlavorDB | nodes | `{"flavors": ["sweet", "bitter"]}` | Flavor descriptions on chemical entities |
| All others | | `{}` | Nothing |

## Per-source details

### FoodOn — food ontology hierarchy

- **Input**: `data/FoodOn/foodon-synonyms.tsv` (a SPARQL export that includes both synonyms and the class hierarchy via the `?parent` column)
- **Nodes**: One per unique `?class` URI. Synonyms grouped by type (`label`, `label_alt`, `exact`, `synonym`, `narrow`, `broad`, `taxon`).
- **Edges**: `is_a` only, from the `?parent` column.
- **Output**: 32,353 nodes, 39,503 edges

### ChEBI — chemical compound ontology

- **Input**: `data/ChEBI/compounds.tsv`, `names.tsv`, `relation.tsv`
- **Nodes**: Top-level compounds (`PARENT_ID` is null). English synonyms merged from `names.tsv`.
- **Edges**: All relationship types from `relation.tsv` (is_a, has_part, etc.).
- **Output**: 205,291 nodes, 374,021 edges

### CDNO — nutrient classification ontology

- **Input**: `data/CDNO/cdno.owl`
- **Nodes**: All non-deprecated OWL classes.
- **Edges**: `is_a` from `rdfs:subClassOf`.
- **Xrefs**: ChEBI IDs from `owl:equivalentClass`. FDC nutrient IDs from `oboInOwl:hasDbXref`.
- **Output**: 2,057 nodes, 2,155 edges, 1,270 xrefs

### CTD — chemical-disease associations

- **Input**: `data/CTD/CTD_chemicals_diseases.csv`, `CTD_diseases.csv`
- **Nodes**: Diseases only. Chemical nodes come from ChEBI.
- **Edges**: Disease `is_a` hierarchy + 9M chemical-disease association edges (vectorized for performance). Each chemdis edge carries `direct_evidence` in raw_attrs — most are empty (inferred), only ~107K have actual values (`"therapeutic"` or `"marker/mechanism"`).
- **Xrefs**: Alternate disease IDs parsed from pipe-delimited `AltDiseaseIDs` (e.g., `"OMIM:264300"` → `target_source="omim"`, `target_id="264300"`).
- **Output**: 13,298 nodes, 9,014,275 edges, 8,066 xrefs

### MeSH — medical subject headings

- **Input**: `data/MeSH/desc*.xml`, `supp*.xml` (glob, no hardcoded year)
- **Nodes**: Descriptors + supplemental records, with synonyms from all concepts/terms.
- **Edges**: `tree_parent` from tree numbers. `mapped_to` from supplemental → descriptor links.
- **Output**: 354,445 nodes, 500,356 edges

### PubChem — cross-reference bridge

- **Input**: `data/PubChem/SID-Map`, `CID-MeSH.txt`
- **Nodes**: None. PubChem is xrefs-only — it bridges ChEBI ↔ PubChem CID ↔ MeSH.
- **Xrefs**: ChEBI → CID from SID-Map. CID → MeSH term from CID-MeSH.
- **Why no nodes**: Compounds without a ChEBI link would be orphans in our graph (no ontology hierarchy to classify them).
- **Output**: 303,182 xrefs

### FlavorDB — flavor compound descriptors

- **Input**: `data/FlavorDB/flavordb_scrape.json`, `data/HSDB/*Odor*.json`, `*Taste*.json`
- **Nodes**: One per PubChem CID with flavor descriptors. HSDB descriptions are fuzzy-matched (90% threshold) against FlavorDB vocabulary before merging.
- **Xrefs**: Each node → its PubChem CID.
- **Output**: 2,407 nodes, 2,407 xrefs

### FDC — USDA food composition data

- **Input**: `data/FDC/FoodData_Central_*/` (glob, no hardcoded version)
- **Nodes**: Foundation foods (`node_type="food"`) + all nutrients (`node_type="nutrient"` with `unit_name` in raw_attrs).
- **Edges**: `contains` — one per food-nutrient pair with concentration `amount` in raw_attrs.
- **Xrefs**: Food → FoodOn URL from `food_attribute.csv`.
- **Output**: 764 nodes, 13,592 edges, 418 xrefs

## CLI

```bash
# Full ingest (all sources in parallel)
uv run python main.py run --stage ingest

# Single source
uv run python main.py run --stage ingest --source foodon

# Multiple sources
uv run python main.py run --stage ingest --source foodon --source chebi
```

## Inspecting output

```bash
uv run python -c "
import pandas as pd
nodes = pd.read_parquet('outputs/ingest/foodon/foodon_nodes.parquet')
print(f'{len(nodes)} rows')
print(nodes.head())
"
```

For raw_attrs (stored as JSON strings):

```bash
uv run python -c "
import pandas as pd, json
edges = pd.read_parquet('outputs/ingest/ctd/ctd_edges.parquet')
edges['raw_attrs'] = edges['raw_attrs'].apply(json.loads)
chemdis = edges[edges['edge_type'] == 'chemical_disease_association']
print(chemdis.iloc[0].to_dict())
"
```
