"""Create empty KG files (parquet for data, JSON for config)."""

import shutil
from pathlib import Path

import pandas as pd

from ..models.relationship import RelationshipType
from ..models.settings import KGCSettings
from ..stores.entity_registry import EntityRegistry
from ..stores.registry_seeder import seed_registry
from ..stores.schema import (
    DIR_INTERMEDIATE,
    FILE_ATTESTATIONS,
    FILE_ENTITIES,
    FILE_EVIDENCE,
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


def _ensure_previous_kg_registry(entities_tsv: Path) -> Path:
    """Generate a registry in the previous KG folder if one doesn't exist."""
    prev_registry = entities_tsv.parent / FILE_REGISTRY
    if prev_registry.exists():
        return prev_registry
    pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(prev_registry, index=False)
    registry = EntityRegistry(prev_registry)
    seed_registry(registry, entities_tsv)
    registry.save()
    return prev_registry


def ensure_registry_exists(settings: KGCSettings) -> None:
    """Create empty ``entity_registry.parquet`` if it does not exist.

    The registry persists across builds and must never be overwritten.
    When *previous_kg_entities* is configured and the registry is new,
    the previous KG's registry is generated (if needed) and copied in.
    """
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)
    path = kg_dir / FILE_REGISTRY
    if path.exists():
        return

    prev_path = settings.previous_kg_entities
    if prev_path:
        prev_registry = _ensure_previous_kg_registry(Path(prev_path))
        shutil.copy2(prev_registry, path)
    else:
        pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(path, index=False)


def create_empty_entity_files(settings: KGCSettings) -> None:
    """Create empty entity-related KG files (entities + LUTs).

    Note: This function must NEVER touch ``entity_registry.parquet``,
    which persists across builds for stable entity ID assignment.
    """
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)
    (kg_dir / DIR_INTERMEDIATE).mkdir(exist_ok=True)

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
    pd.DataFrame().to_parquet(kg_dir / FILE_ATTESTATIONS)
    pd.DataFrame().to_parquet(kg_dir / FILE_RETIRED)
