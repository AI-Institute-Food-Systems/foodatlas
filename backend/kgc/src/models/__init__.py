"""KGC data models."""

from .entity import ChemicalEntity, DiseaseEntity, Entity, FlavorEntity, FoodEntity
from .metadata import MetadataContains, MetadataDisease, MetadataFlavor
from .relationship import Relationship, RelationshipType
from .settings import KGCSettings
from .triplet import Triplet
from .version import KGVersion

__all__ = [
    "ChemicalEntity",
    "DiseaseEntity",
    "Entity",
    "FlavorEntity",
    "FoodEntity",
    "KGCSettings",
    "KGVersion",
    "MetadataContains",
    "MetadataDisease",
    "MetadataFlavor",
    "Relationship",
    "RelationshipType",
    "Triplet",
]
