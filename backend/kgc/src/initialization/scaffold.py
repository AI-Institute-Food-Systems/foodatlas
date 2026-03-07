"""Create empty KG files as JSON."""

import json
from pathlib import Path

from ..models.relationship import RelationshipType
from ..models.settings import KGCSettings
from ..stores.schema import (
    FILE_ENTITIES,
    FILE_LUT_CHEMICAL,
    FILE_LUT_FOOD,
    FILE_METADATA_CONTAINS,
    FILE_RELATIONSHIPS,
    FILE_RETIRED,
    FILE_TRIPLETS,
)


def _build_default_relationships() -> list[dict[str, str]]:
    return [
        {"foodatlas_id": rt.value, "name": rt.name.lower()} for rt in RelationshipType
    ]


def _write_json(path: Path, data: object) -> None:
    with path.open("w") as f:
        json.dump(data, f, ensure_ascii=False)


def create_empty_files(settings: KGCSettings) -> None:
    """Create all empty KG files as JSON."""
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)

    _write_json(kg_dir / FILE_ENTITIES, [])
    _write_json(kg_dir / FILE_RELATIONSHIPS, _build_default_relationships())
    _write_json(kg_dir / FILE_TRIPLETS, [])
    _write_json(kg_dir / FILE_METADATA_CONTAINS, [])
    _write_json(kg_dir / FILE_RETIRED, [])
    _write_json(kg_dir / FILE_LUT_FOOD, {})
    _write_json(kg_dir / FILE_LUT_CHEMICAL, {})
