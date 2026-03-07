"""MetadataContains model for the knowledge graph."""

from __future__ import annotations

from pydantic import BaseModel


class MetadataContains(BaseModel):
    foodatlas_id: str
    conc_value: float | None = None
    conc_unit: str = ""
    food_part: str = ""
    food_processing: str = ""
    source: str = ""
    reference: list[str] = []
    entity_linking_method: str = ""
    quality_score: float | None = None
    food_name_raw: str = ""
    chemical_name_raw: str = ""
    conc_raw: str = ""
    food_part_raw: str = ""
