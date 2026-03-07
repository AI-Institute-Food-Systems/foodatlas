"""Entity creation logic for the knowledge graph."""

from .chemical import create_chemical_entities
from .food import create_food_entities

__all__ = [
    "create_chemical_entities",
    "create_food_entities",
]
