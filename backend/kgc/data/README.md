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
- `Lit2KG/` — LLM-extracted sentences and metadata from literature.
- `MeSH/` — Medical Subject Headings ontology.
- `PubChem/` — Chemical property records.

After the script completes, confirm the directories exist:

```console
ls data
```
