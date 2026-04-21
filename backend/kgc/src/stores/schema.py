"""Schema constants and column definitions for KG files."""

from pydantic import BaseModel

from ..models.attestation import Attestation
from ..models.entity import Entity
from ..models.evidence import Evidence
from ..models.triplet import Triplet

INDEX_COL = "foodatlas_id"

FILE_ENTITIES = "entities.parquet"
FILE_TRIPLETS = "triplets.parquet"
FILE_EVIDENCE = "evidence.parquet"
FILE_ATTESTATIONS = "attestations.parquet"
FILE_RELATIONSHIPS = "relationships.parquet"
DIR_INTERMEDIATE = "intermediate"
FILE_LUT_FOOD = "intermediate/lookup_table_food.json"
FILE_LUT_CHEMICAL = "intermediate/lookup_table_chemical.json"

RELATIONSHIP_COLUMNS = ["foodatlas_id", "name"]
LUT_COLUMNS = ["name", "foodatlas_id"]

FILE_REGISTRY = "entity_registry.parquet"
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
ATTESTATION_COLUMNS = _get_columns(Attestation)

DIR_DIAGNOSTICS = "diagnostics"
FILE_IE_UNRESOLVED = "diagnostics/ie_unresolved.jsonl"
FILE_IE_PARSE_ERRORS = "diagnostics/ie_parse_errors.tsv"
FILE_IE_CONC_ERRORS = "diagnostics/ie_conc_unconverted.tsv"
FILE_ATTESTATIONS_AMBIGUOUS = "attestations_ambiguous.parquet"
