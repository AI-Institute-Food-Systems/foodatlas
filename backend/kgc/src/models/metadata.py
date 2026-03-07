"""MetadataContains model for the knowledge graph."""

from pydantic import BaseModel, ConfigDict, Field


class MetadataContains(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    foodatlas_id: str
    conc_value: float | None = None
    conc_unit: str = ""
    food_part: str = ""
    food_processing: str = ""
    source: str = ""
    reference: list[str] = []
    entity_linking_method: str = ""
    quality_score: float | None = None
    food_name_raw: str = Field(default="", alias="_food_name")
    chemical_name_raw: str = Field(default="", alias="_chemical_name")
    conc_raw: str = Field(default="", alias="_conc")
    food_part_raw: str = Field(default="", alias="_food_part")
