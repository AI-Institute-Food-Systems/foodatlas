"""Relationship model for the knowledge graph."""

from enum import StrEnum

from pydantic import BaseModel


class RelationshipType(StrEnum):
    CONTAINS = "r1"
    IS_A = "r2"
    POSITIVELY_CORRELATES_WITH = "r3"
    NEGATIVELY_CORRELATES_WITH = "r4"
    HAS_FLAVOR = "r5"


class Relationship(BaseModel):
    relationship_id: str
    relationship_type: RelationshipType
    description: str = ""
