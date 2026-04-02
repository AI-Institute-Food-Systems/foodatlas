"""Create empty KG files (parquet for data, JSON for config)."""

from pathlib import Path

import pandas as pd

from ..models.relationship import RelationshipType
from ..models.settings import KGCSettings
from ..stores.schema import (
    FILE_ENTITIES,
    FILE_EVIDENCE,
    FILE_EXTRACTIONS,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_REGISTRY,
    FILE_RELATIONSHIPS,
    FILE_RETIRED,
    FILE_TRIPLETS,
    REGISTRY_COLUMNS,
)
from ..utils.json_io import write_json


def _build_default_relationships() -> list[dict[str, str]]:
    return [
        {"foodatlas_id": rt.value, "name": rt.name.lower()} for rt in RelationshipType
    ]


def ensure_registry_exists(settings: KGCSettings) -> None:
    """Create empty ``entity_registry.parquet`` if it does not exist.

    The registry persists across builds and must never be overwritten.
    """
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)
    path = kg_dir / FILE_REGISTRY
    if not path.exists():
        pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(path, index=False)


def create_empty_entity_files(settings: KGCSettings) -> None:
    """Create empty entity-related KG files (entities + LUTs).

    Note: This function must NEVER touch ``entity_registry.parquet``,
    which persists across builds for stable entity ID assignment.
    """
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame().to_parquet(kg_dir / FILE_ENTITIES)
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})


def create_empty_triplet_files(settings: KGCSettings) -> None:
    """Create empty triplet-related KG files (triplets, metadata, etc.)."""
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(_build_default_relationships()).to_parquet(
        kg_dir / FILE_RELATIONSHIPS, index=False
    )
    pd.DataFrame().to_parquet(kg_dir / FILE_TRIPLETS)
    pd.DataFrame().to_parquet(kg_dir / FILE_EVIDENCE)
    pd.DataFrame().to_parquet(kg_dir / FILE_EXTRACTIONS)
    pd.DataFrame().to_parquet(kg_dir / FILE_RETIRED)
