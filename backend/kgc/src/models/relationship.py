"""Relationship model for the knowledge graph."""

from enum import StrEnum

from pydantic import BaseModel


class RelationshipType(StrEnum):
    CONTAINS = "r1"
    IS_A = "r2"
    PART_OF = "r3"
    HAS_PROPERTY = "r4"
    RELATED_TO = "r5"


class Relationship(BaseModel):
    relationship_id: str
    relationship_type: RelationshipType
    description: str = ""
