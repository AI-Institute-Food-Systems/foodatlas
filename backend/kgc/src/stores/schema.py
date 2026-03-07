"""Schema constants and column definitions for KG TSV files."""

from pydantic import BaseModel

from ..models.entity import Entity
from ..models.metadata import MetadataContains
from ..models.triplet import Triplet

TSV_SEP = "\t"
INDEX_COL = "foodatlas_id"

FILE_ENTITIES = "entities.tsv"
FILE_TRIPLETS = "triplets.tsv"
FILE_METADATA_CONTAINS = "metadata_contains.tsv"
FILE_LUT_FOOD = "lookup_table_food.json"
FILE_LUT_CHEMICAL = "lookup_table_chemical.json"


def _get_columns(model: type[BaseModel]) -> list[str]:
    """Get TSV column names from a model, respecting aliases."""
    cols: list[str] = []
    for name, field_info in model.model_fields.items():
        alias = field_info.alias
        cols.append(alias if alias else name)
    return cols


ENTITY_COLUMNS = _get_columns(Entity)
TRIPLET_COLUMNS = _get_columns(Triplet)
METADATA_CONTAINS_COLUMNS = _get_columns(MetadataContains)
