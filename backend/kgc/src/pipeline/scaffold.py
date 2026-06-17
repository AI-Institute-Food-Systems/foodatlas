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
    FILE_TRIPLETS,
    REGISTRY_COLUMNS,
)
from ..utils.json_io import write_json
from ..utils.snapshots import latest_snapshot


def _build_default_relationships() -> list[dict[str, str]]:
    return [
        {"foodatlas_id": rt.value, "name": rt.name.lower()} for rt in RelationshipType
    ]


def _ensure_previous_kg_registry(entities_path: Path) -> Path:
    """Return the registry next to *entities_path*, building it if absent.

    A snapshot already carrying ``entity_registry.parquet`` is used as-is;
    otherwise the registry is seeded from the sibling entities file.
    """
    prev_registry = entities_path.parent / FILE_REGISTRY
    if prev_registry.exists():
        return prev_registry
    pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(prev_registry, index=False)
    registry = EntityRegistry(prev_registry)
    seed_registry(registry, entities_path)
    registry.save()
    return prev_registry


def _resolve_seed_registry(settings: KGCSettings) -> Path | None:
    """Find the previous-KG registry to seed foodatlas_ids from.

    Uses the explicit ``previous_kg_entities`` path when set; otherwise
    auto-discovers the latest ``PreviousFAKG/`` snapshot. Returns None when
    there is no previous KG (a fresh build seeds an empty registry).
    """
    explicit = settings.previous_kg_entities
    if explicit:
        return _ensure_previous_kg_registry(Path(explicit))

    snapshot = latest_snapshot(settings.data_dir)
    if snapshot is None:
        return None
    registry = snapshot / FILE_REGISTRY
    if not registry.exists():
        msg = f"Snapshot {snapshot} has no {FILE_REGISTRY}; cannot seed registry"
        raise FileNotFoundError(msg)
    return registry


def ensure_registry_exists(settings: KGCSettings) -> None:
    """Seed ``entity_registry.parquet`` from the previous KG.

    Always re-seeds so the registry is deterministic and not affected by
    leftover state from prior runs.
    """
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)
    path = kg_dir / FILE_REGISTRY

    seed_registry_path = _resolve_seed_registry(settings)
    if seed_registry_path is not None:
        shutil.copy2(seed_registry_path, path)
    else:
        pd.DataFrame(columns=REGISTRY_COLUMNS).to_parquet(path, index=False)


def create_empty_entity_files(settings: KGCSettings) -> None:
    """Create empty entity-related KG files (entities + LUTs)."""
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
