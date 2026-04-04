"""Triplet model for the knowledge graph."""

from pydantic import BaseModel, Field


class Triplet(BaseModel):
    head_id: str
    relationship_id: str
    tail_id: str
    source: str = ""
    extraction_ids: list[str] = Field(default_factory=list)
