"""Pydantic models for the public /v1/ API.

These models are the *public contract*. Keep them stable; if the shape needs
to change, version up to /v2/ rather than editing in place.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class Page(BaseModel):
    """Pagination metadata returned with every list response."""

    page: int = Field(..., ge=1, description="1-indexed page number")
    page_size: int = Field(..., ge=1, le=100)
    total: int = Field(..., ge=0, description="Total matching rows across all pages")
    has_more: bool


class ListResponse[T](BaseModel):
    data: list[T]
    page: Page


class ItemResponse[T](BaseModel):
    data: T


class ExternalIds(BaseModel):
    """Raw external identifiers grouped by source (e.g. ``chebi``, ``fdc``)."""

    model_config = {"extra": "allow"}


class FoodSummary(BaseModel):
    id: str = Field(..., description="FoodAtlas id, e.g. FA:0001")
    common_name: str
    scientific_name: str = ""
    food_classification: list[str] = Field(default_factory=list)


class Food(FoodSummary):
    synonyms: list[str] = Field(default_factory=list)
    external_ids: dict[str, list[str]] = Field(default_factory=dict)


class ChemicalSummary(BaseModel):
    id: str
    common_name: str
    scientific_name: str = ""
    chemical_classification: list[str] = Field(default_factory=list)


class Chemical(ChemicalSummary):
    synonyms: list[str] = Field(default_factory=list)
    flavor_descriptors: list[str] = Field(default_factory=list)
    external_ids: dict[str, list[str]] = Field(default_factory=dict)


class DiseaseSummary(BaseModel):
    id: str
    common_name: str
    scientific_name: str = ""


class Disease(DiseaseSummary):
    synonyms: list[str] = Field(default_factory=list)
    external_ids: dict[str, list[str]] = Field(default_factory=dict)


class BioactivityHierarchyNode(BaseModel):
    """One entry in ``parents`` / ``children`` of a Bioactivity."""

    foodatlas_id: str
    common_name: str


class BioactivitySummary(BaseModel):
    id: str
    common_name: str
    description: str = ""
    n_foods: int = 0
    n_chemicals: int = 0


class Bioactivity(BioactivitySummary):
    synonyms: list[str] = Field(default_factory=list)
    external_ids: dict[str, list[str]] = Field(default_factory=dict)
    parents: list[BioactivityHierarchyNode] = Field(default_factory=list)
    children: list[BioactivityHierarchyNode] = Field(default_factory=list)


class BioactivityMeasurement(BaseModel):
    """Highest-value sample measurement for a bioactivity row."""

    endpoint: str = ""
    value: float | None = None
    unit: str = ""


class BioactivityChemicalRow(BaseModel):
    """Flat row for /v1/bioactivities/{id}/chemicals and
    /v1/chemicals/{id}/bioactivities.
    """

    bioactivity_id: str
    bioactivity_name: str
    chemical_id: str
    chemical_name: str
    measurement_count: int = 0
    active_count: int = 0
    inactive_count: int = 0
    top_measurement: BioactivityMeasurement | None = None


class BioactivityFoodRow(BaseModel):
    """Flat row for /v1/bioactivities/{id}/foods and
    /v1/foods/{id}/bioactivities.
    """

    bioactivity_id: str
    bioactivity_name: str
    food_id: str
    food_name: str
    measurement_count: int = 0
    top_measurement: BioactivityMeasurement | None = None


class Concentration(BaseModel):
    value: float
    unit: str


class CompositionRow(BaseModel):
    """Flat row for /v1/foods/{id}/chemicals and /v1/chemicals/{id}/foods.

    Aggregates evidence across all sources (FDC, FoodAtlas literature, DMD)
    into a single ``sources[]`` list + ``attestation_count`` — no UI-specific
    per-source grouping.
    """

    food_id: str
    food_name: str
    chemical_id: str
    chemical_name: str
    chemical_classification: list[str] = Field(default_factory=list)
    median_concentration: Concentration | None = None
    attestation_count: int = 0
    sources: list[str] = Field(default_factory=list)


class CorrelationRow(BaseModel):
    """Flat row for /v1/chemicals/{id}/diseases and /v1/diseases/{id}/chemicals.

    The ``relation`` field is one of ``reduces`` (r4) or ``worsens`` (r3).
    """

    chemical_id: str
    chemical_name: str
    disease_id: str
    disease_name: str
    relation: Literal["reduces", "worsens"]
    source_chemical_id: str = ""
    source_chemical_name: str = ""
    sources: list[str] = Field(default_factory=list)
    evidence_count: int = 0


class AttestationSummary(BaseModel):
    attestation_id: str
    source: str
    evidence_id: str
    trust_score: float | None = None


class Attestation(AttestationSummary):
    head_id: str
    head_name_raw: str
    tail_id: str
    tail_name_raw: str
    relationship_id: str
    conc_value: float | None = None
    conc_unit: str = ""
    food_part: str = ""
    food_processing: str = ""
    validated: bool = False
    validated_correct: bool = True
    trust_reason: str = ""


class Triplet(BaseModel):
    triplet_id: int
    head_id: str
    head_name: str
    relationship_id: str
    relationship_name: str
    tail_id: str
    tail_name: str
    source: str = ""
    attestations: list[AttestationSummary] = Field(default_factory=list)


class TaxonomyNode(BaseModel):
    id: str
    name: str
    has_page: bool


class TaxonomyEdge(BaseModel):
    child_id: str
    parent_id: str


class Taxonomy(BaseModel):
    entity_id: str | None
    nodes: list[TaxonomyNode]
    edges: list[TaxonomyEdge]


class SearchHit(BaseModel):
    id: str
    common_name: str
    entity_type: Literal["food", "chemical", "disease", "bioactivity"]
    scientific_name: str = ""
    associations: int = 0


class Stats(BaseModel):
    foods: int = 0
    chemicals: int = 0
    diseases: int = 0
    bioactivities: int = 0
    bioactivity_measurements: int = 0
    publications: int = 0
    connections: int = 0


class Bundle(BaseModel):
    version: str
    release_date: str
    file_size: str = ""
    kgc_run: str = ""
    download_link: str
    summary_link: str = ""
