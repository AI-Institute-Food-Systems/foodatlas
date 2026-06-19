"""KGC data models."""

from .attestation import Attestation
from .entity import (
    BioactivityEntity,
    ChemicalEntity,
    DiseaseEntity,
    Entity,
    FoodEntity,
)
from .evidence import Evidence
from .relationship import Relationship, RelationshipType
from .settings import KGCSettings
from .triplet import Triplet
from .version import KGVersion

__all__ = [
    "Attestation",
    "BioactivityEntity",
    "ChemicalEntity",
    "DiseaseEntity",
    "Entity",
    "Evidence",
    "FoodEntity",
    "KGCSettings",
    "KGVersion",
    "Relationship",
    "RelationshipType",
    "Triplet",
]
