# FoodAtlas KGC

Knowledge graph construction component for FoodAtlas.

## Getting Started

### Install dependencies

```bash
uv sync
```

### Run

```bash
uv run python main.py
```

### Run tests

```bash
uv run pytest
```

## Project Structure

```
kgc/
├── pyproject.toml
├── main.py
├── src/
│   ├── __init__.py
│   ├── config/              # Settings (paths, API keys)
│   ├── models/              # Pydantic data models (Entity, Triplet, Metadata, etc.)
│   ├── stores/              # Runtime containers wrapping pandas DataFrames
│   │   ├── schema.py        # Column definitions derived from models, file/format constants
│   │   ├── entity_store.py  # Entity storage with synonym lookup tables (JSON)
│   │   ├── triplet_store.py # Triplet storage with deduplication
│   │   └── metadata_store.py
│   ├── discovery/            # Runtime entity creation from external sources
│   │   ├── query.py          # API stubs for NCBI Taxonomy & PubChem
│   │   ├── food.py           # Food entity creation (NCBI + synonym grouping)
│   │   └── chemical.py       # Chemical entity creation (PubChem + name lookup)
│   ├── preprocessing/        # Text normalization constants (Greek letters, units, etc.)
│   └── utils/                # Shared helpers (merge_sets, lookup key formatting)
└── tests/
    ├── conftest.py           # Shared fixtures (populated/empty EntityStore)
    ├── test_entities.py
    ├── test_food_entities.py
    ├── test_chemical_entities.py
    ├── test_triplets.py
    ├── test_metadata.py
    ├── test_models.py
    └── ...
```

## Architecture

### Models → Schema → Stores

Pydantic models in `src/models/` are the single source of truth for the KG schema.
`src/stores/schema.py` derives TSV column lists from model fields (respecting aliases),
so column definitions are never duplicated.

- **Models** define field names, types, defaults, and aliases (e.g., `synonyms_display` → `_synonyms_display` in TSV)
- **Stores** load/save TSV files and manage in-memory DataFrames + lookup structures
- **Discovery** creates new entities at runtime by querying NCBI Taxonomy (food) or PubChem (chemical)

### Data Files

| File | Format | Description |
|------|--------|-------------|
| `entities.tsv` | TSV | All food & chemical entities |
| `triplets.tsv` | TSV | Relationship triplets (head, rel, tail) |
| `metadata_contains.tsv` | TSV | Metadata for "contains" relationships |
| `lookup_table_food.json` | JSON | Synonym → entity ID mapping for foods |
| `lookup_table_chemical.json` | JSON | Synonym → entity ID mapping for chemicals |
