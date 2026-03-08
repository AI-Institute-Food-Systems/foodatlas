"""Create empty KG files as JSON."""

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
from ..utils.json_io import write_json


def _build_default_relationships() -> list[dict[str, str]]:
    return [
        {"foodatlas_id": rt.value, "name": rt.name.lower()} for rt in RelationshipType
    ]


def create_empty_files(settings: KGCSettings) -> None:
    """Create all empty KG files as JSON."""
    kg_dir = Path(settings.kg_dir)
    kg_dir.mkdir(parents=True, exist_ok=True)

    write_json(kg_dir / FILE_ENTITIES, [])
    write_json(kg_dir / FILE_RELATIONSHIPS, _build_default_relationships())
    write_json(kg_dir / FILE_TRIPLETS, [])
    write_json(kg_dir / FILE_METADATA_CONTAINS, [])
    write_json(kg_dir / FILE_RETIRED, [])
    write_json(kg_dir / FILE_LUT_FOOD, {})
    write_json(kg_dir / FILE_LUT_CHEMICAL, {})
