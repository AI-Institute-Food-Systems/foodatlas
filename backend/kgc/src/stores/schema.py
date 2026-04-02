"""Schema constants and column definitions for KG files."""

from pydantic import BaseModel

from ..models.entity import Entity
from ..models.evidence import Evidence
from ..models.extraction import Extraction
from ..models.triplet import Triplet

INDEX_COL = "foodatlas_id"

FILE_ENTITIES = "entities.parquet"
FILE_TRIPLETS = "triplets.parquet"
FILE_EVIDENCE = "evidence.parquet"
FILE_EXTRACTIONS = "extractions.parquet"
FILE_RELATIONSHIPS = "relationships.parquet"
FILE_LUT_FOOD = "_lookup_table_food.json"
FILE_LUT_CHEMICAL = "_lookup_table_chemical.json"
FILE_RETIRED = "retired.parquet"

RELATIONSHIP_COLUMNS = ["foodatlas_id", "name"]
LUT_COLUMNS = ["name", "foodatlas_id"]
RETIRED_COLUMNS = ["foodatlas_id", "action", "destination"]

FILE_REGISTRY = "entity_registry.parquet"
FILE_BUILD_DIFF = "_build_diff.json"
REGISTRY_COLUMNS = ["source", "native_id", "foodatlas_id"]


def _get_columns(model: type[BaseModel]) -> list[str]:
    """Get column names from a model, respecting aliases."""
    cols: list[str] = []
    for name, field_info in model.model_fields.items():
        alias = field_info.alias
        cols.append(alias if alias else name)
    return cols


ENTITY_COLUMNS = _get_columns(Entity)
TRIPLET_COLUMNS = _get_columns(Triplet)
EVIDENCE_COLUMNS = _get_columns(Evidence)
EXTRACTION_COLUMNS = _get_columns(Extraction)

FILE_IE_UNRESOLVED = "_ie_unresolved.tsv"
FILE_IE_RESOLUTION_STATS = "_ie_resolution_stats.json"
