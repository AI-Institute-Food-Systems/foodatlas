"""Triplet model for the knowledge graph."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Triplet(BaseModel):
    foodatlas_id: str
    head_id: str
    relationship_id: str
    tail_id: str
    metadata_ids: list[str] = Field(default_factory=list)
