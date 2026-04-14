# FoodAtlas Data Workspace

This directory holds the reference ontologies, curated datasets, and model outputs required to rebuild the FoodAtlas knowledge graph. Most public resources can be fetched automatically; a few restricted datasets must be supplied manually.

## 1. Prerequisites

Ensure you have `curl` and `unzip` installed. On Ubuntu:

```console
sudo apt-get update && sudo apt-get install curl unzip
```

## 2. Download supported archives

Run the helper script from the repository root or directly inside `data/`:

```console
cd data
./download.sh
cd ..
```

The script retrieves and unpacks the following archives into this directory:

- `CDNO/` — Common Data on Nutrition Ontology snapshot.
- `ChEBI/` — Chemical Entities of Biological Interest export curated for FoodAtlas.
- `CTD/` — Comparative Toxicogenomics Database subset (chemical–disease associations).
- `FDC/` — FoodData Central aggregates used for nutrition metadata.
- `FlavorDB/` — Flavor compound information.
- `FlavorGraph/` — Graph representation of ingredient–flavor relationships.
- `FoodOn/` — Food ontology for hierarchical grouping.
- `HSDB/` — Hazardous Substances Data Bank export.
- `MeSH/` — Medical Subject Headings ontology.
- `PubChem/` — Chemical property records.

> **`Lit2KG/` is deprecated.** The directory may exist on disk from older runs but the current pipeline does not read from it; literature inputs flow from [`backend/ie/`](../../ie/) instead. It is excluded from the S3 sync. Safe to leave in place or delete.

> **`PreviousFAKG/` is special.** Holds the previous KG run as the baseline for the next KGC build. It is *not* uploaded by `sync-data-to-s3.sh` (that would create an upload loop). Instead, populate it with `../scripts/pull-from-s3.sh`, which downloads `s3://<bucket>/outputs/<latest>/kg/` into `PreviousFAKG/<ts>/`.

After the script completes, confirm the directories exist:

```console
ls data
```

## Publishing to S3 (production)

Once a KGC run has finalized this `data/` directory and produced fresh outputs, the source ontologies can be published to S3 for traceability and reuse by other developers:

```bash
../scripts/sync-data-to-s3.sh        # uploads data/ → s3://<bucket>/data/<UTC-ts>/
```

The script excludes `PreviousFAKG/`, `Lit2KG/`, and repo housekeeping (`README.md`, `.gitignore`, `download.sh`). See [`infra/README.md`](../../../infra/README.md) for the full S3 layout and version-pinning workflow.
