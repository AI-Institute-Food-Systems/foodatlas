"""Entity models matching the KG entity schema."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Entity(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    foodatlas_id: str
    entity_type: Literal["food", "chemical", "disease", "flavor"]
    common_name: str
    scientific_name: str = ""
    synonyms: list[str] = Field(default_factory=list)
    external_ids: dict[str, list[str]] = Field(default_factory=dict)
    synonyms_display: list[str] = Field(
        default_factory=list,
        alias="_synonyms_display",
    )


class FoodEntity(Entity):
    entity_type: Literal["food"] = "food"


class ChemicalEntity(Entity):
    entity_type: Literal["chemical"] = "chemical"


class DiseaseEntity(Entity):
    entity_type: Literal["disease"] = "disease"


class FlavorEntity(Entity):
    entity_type: Literal["flavor"] = "flavor"
