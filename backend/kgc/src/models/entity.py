"""Entity models matching the KG entity schema."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class Entity(BaseModel):
    foodatlas_id: str
    entity_type: Literal["food", "chemical"]
    common_name: str
    scientific_name: str = ""
    synonyms: list[str] = Field(default_factory=list)
    external_ids: dict[str, list[str]] = Field(default_factory=dict)
    synonyms_display: list[str] = Field(default_factory=list)


class FoodEntity(Entity):
    entity_type: Literal["food"] = "food"


class ChemicalEntity(Entity):
    entity_type: Literal["chemical"] = "chemical"
