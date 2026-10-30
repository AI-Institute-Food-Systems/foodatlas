"""Typed attribute models for entity-type-specific metadata.

Each entity type defines its own attributes model.  Enrichment code
validates through these models before writing; the entity DataFrame
stores the result as a plain ``dict`` in the ``attributes`` column.

Source adapters also park provenance on ``attributes`` (e.g. an FDC food's
data type, NDB number and USDA FoodOn mapping). Those keys are not modelled
here, so the models keep unknown keys instead of dropping them: a
read-modify-write of ``food_groups`` must not erase what ingest recorded.
"""

from pydantic import BaseModel, ConfigDict, Field


class EntityAttributes(BaseModel):
    """Base attributes — empty by default; unknown keys pass through."""

    model_config = ConfigDict(extra="allow")


class ChemicalAttributes(EntityAttributes):
    """Attributes specific to chemical entities."""

    chemical_groups: list[str] = Field(default_factory=list)
    flavor_descriptors: list[str] = Field(default_factory=list)


class FoodAttributes(EntityAttributes):
    """Attributes specific to food entities."""

    food_groups: list[str] = Field(default_factory=list)


class DiseaseAttributes(EntityAttributes):
    """Attributes specific to disease entities."""
