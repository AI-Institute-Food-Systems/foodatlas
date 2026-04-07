"""Typed attribute models for entity-type-specific metadata.

Each entity type defines its own attributes model.  Enrichment code
validates through these models before writing; the entity DataFrame
stores the result as a plain ``dict`` in the ``attributes`` column.
"""

from pydantic import BaseModel, Field


class EntityAttributes(BaseModel):
    """Base attributes — empty by default."""


class ChemicalAttributes(EntityAttributes):
    """Attributes specific to chemical entities."""

    chemical_groups: list[str] = Field(default_factory=list)
    flavor_descriptors: list[str] = Field(default_factory=list)


class FoodAttributes(EntityAttributes):
    """Attributes specific to food entities."""

    food_groups: list[str] = Field(default_factory=list)


class DiseaseAttributes(EntityAttributes):
    """Attributes specific to disease entities."""
