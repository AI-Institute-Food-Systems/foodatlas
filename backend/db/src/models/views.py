"""Materialized API tables — denormalized, computed during ETL.

These tables are truncated and re-populated on each ETL run.
The API layer queries only these tables, not the base tables.
"""

from sqlalchemy import BigInteger, Double, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class MVFoodEntity(Base):
    """Food entities enriched with FoodOn classification."""

    __tablename__ = "mv_food_entities"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    common_name: Mapped[str] = mapped_column(Text, nullable=False)
    scientific_name: Mapped[str] = mapped_column(Text, server_default="")
    synonyms: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    external_ids: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    food_classification: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )
    ambiguity_siblings: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (Index("ix_mv_food_common_name", "common_name"),)


class MVChemicalEntity(Base):
    """Chemical entities enriched with ChEBI classification."""

    __tablename__ = "mv_chemical_entities"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    common_name: Mapped[str] = mapped_column(Text, nullable=False)
    scientific_name: Mapped[str] = mapped_column(Text, server_default="")
    synonyms: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    external_ids: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    chemical_classification: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )
    flavor_descriptors: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )
    ambiguity_siblings: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (Index("ix_mv_chem_common_name", "common_name"),)


class MVDiseaseEntity(Base):
    """Disease entities that appear in correlation triplets."""

    __tablename__ = "mv_disease_entities"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    common_name: Mapped[str] = mapped_column(Text, nullable=False)
    scientific_name: Mapped[str] = mapped_column(Text, server_default="")
    synonyms: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    external_ids: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    ambiguity_siblings: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (Index("ix_mv_disease_common_name", "common_name"),)


class MVBioactivityEntity(Base):
    """Bioactivity entities that appear in r5/r6/r7 triplets.

    No parent_classification — each MeSH is treated independently
    (Pranav, 2026-06-05).
    """

    __tablename__ = "mv_bioactivity_entities"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    common_name: Mapped[str] = mapped_column(Text, nullable=False)
    scientific_name: Mapped[str] = mapped_column(Text, server_default="")
    synonyms: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    external_ids: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    description: Mapped[str] = mapped_column(Text, server_default="")
    ambiguity_siblings: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (Index("ix_mv_bioactivity_common_name", "common_name"),)


class MVChemicalBioactivityMeasurement(Base):
    """Denormalized chemical-bioactivity measurements (r5).

    One row per (chemical, bioactivity); ``measurements`` aggregates the
    underlying assay attestations with potency/efficacy/source fields.
    """

    __tablename__ = "mv_chemical_bioactivity_measurement"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chemical_name: Mapped[str] = mapped_column(Text, nullable=False)
    chemical_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    bioactivity_name: Mapped[str] = mapped_column(Text, nullable=False)
    bioactivity_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    measurement_count: Mapped[int] = mapped_column(Integer, server_default="0")
    measurements: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (
        Index("ix_mv_cbm_chemical", "chemical_foodatlas_id"),
        Index("ix_mv_cbm_bioactivity", "bioactivity_foodatlas_id"),
    )


class MVFoodBioactivityExhibits(Base):
    """Denormalized food-bioactivity exhibits (r6, direct + inherited).

    One row per (food, bioactivity, exhibit_type). For inherited rows,
    ``via_chemical_id`` / ``via_chemical_name`` identify the bridging
    chemical and ``efficacy_pred`` carries the Hill-curve prediction
    computed during materialization.
    """

    __tablename__ = "mv_food_bioactivity_exhibits"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    food_name: Mapped[str] = mapped_column(Text, nullable=False)
    food_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    bioactivity_name: Mapped[str] = mapped_column(Text, nullable=False)
    bioactivity_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    exhibit_type: Mapped[str] = mapped_column(String(20), nullable=False)
    via_chemical_id: Mapped[str | None] = mapped_column(String(20), nullable=True)
    via_chemical_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    efficacy_pred: Mapped[float | None] = mapped_column(Double, nullable=True)
    evidence_count: Mapped[int] = mapped_column(Integer, server_default="0")
    evidences: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (
        Index("ix_mv_fbe_food", "food_foodatlas_id"),
        Index("ix_mv_fbe_bioactivity", "bioactivity_foodatlas_id"),
        Index("ix_mv_fbe_exhibit_type", "exhibit_type"),
    )


class MVBioactivityDiseaseAssociation(Base):
    """Denormalized bioactivity-disease associations (r7).

    One row per (bioactivity, disease); ``target_ids`` aggregates UniProt
    target ids across all backing BDM rows.
    """

    __tablename__ = "mv_bioactivity_disease_association"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    bioactivity_name: Mapped[str] = mapped_column(Text, nullable=False)
    bioactivity_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    disease_name: Mapped[str] = mapped_column(Text, nullable=False)
    disease_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    polarity: Mapped[str | None] = mapped_column(String(20), nullable=True)
    target_ids: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    evidence_count: Mapped[int] = mapped_column(Integer, server_default="0")
    evidences: Mapped[list] = mapped_column(JSONB, server_default="[]")

    __table_args__ = (
        Index("ix_mv_bda_bioactivity", "bioactivity_foodatlas_id"),
        Index("ix_mv_bda_disease", "disease_foodatlas_id"),
    )


class MVFoodChemicalComposition(Base):
    """Denormalized food-chemical composition with evidence.

    One row per food-chemical pair from r1 (CONTAINS) triplets.
    Evidence is pre-structured to match the frontend FoodEvidence type.
    """

    __tablename__ = "mv_food_chemical_composition"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    food_name: Mapped[str] = mapped_column(Text, nullable=False)
    food_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    chemical_name: Mapped[str] = mapped_column(Text, nullable=False)
    chemical_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    chemical_classification: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )
    median_concentration: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    fdc_evidences: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    foodatlas_evidences: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    dmd_evidences: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_mv_fcc_food_name", "food_name"),
        Index("ix_mv_fcc_chemical_name", "chemical_name"),
    )


class MVChemicalDiseaseCorrelation(Base):
    """Denormalized chemical-disease correlations with evidence.

    One row per (chemical, source_chemical, disease, relationship).
    For direct connections source_chemical = chemical itself.
    For inherited connections source_chemical = the descendant chemical.
    """

    __tablename__ = "mv_chemical_disease_correlation"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chemical_name: Mapped[str] = mapped_column(Text, nullable=False)
    chemical_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    relationship_id: Mapped[str] = mapped_column(String(10), nullable=False)
    disease_name: Mapped[str] = mapped_column(Text, nullable=False)
    disease_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    source_chemical_name: Mapped[str] = mapped_column(Text, server_default="")
    source_chemical_foodatlas_id: Mapped[str] = mapped_column(
        String(20), server_default=""
    )
    sources: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    evidences: Mapped[list] = mapped_column(JSONB, server_default="[]")
    evidence_count: Mapped[int] = mapped_column(Integer, server_default="0")

    __table_args__ = (
        Index("ix_mv_cdc_chem_rel", "chemical_name", "relationship_id"),
        Index("ix_mv_cdc_disease_rel", "disease_name", "relationship_id"),
    )


class MVSearchAutoComplete(Base):
    """Pre-computed search index for autocomplete.

    Uses pg_trgm for substring similarity ranking.
    """

    __tablename__ = "mv_search_auto_complete"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    associations: Mapped[int] = mapped_column(Integer, server_default="0")
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    common_name: Mapped[str] = mapped_column(Text, nullable=False)
    scientific_name: Mapped[str] = mapped_column(Text, server_default="")
    synonyms: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    external_ids: Mapped[dict] = mapped_column(JSONB, server_default="{}")
    exact_auto: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    substr_auto: Mapped[str] = mapped_column(Text, server_default="")

    __table_args__ = (
        Index(
            "ix_mv_search_substr_trgm",
            "substr_auto",
            postgresql_using="gin",
            postgresql_ops={"substr_auto": "gin_trgm_ops"},
        ),
        Index(
            "ix_mv_search_exact",
            "exact_auto",
            postgresql_using="gin",
        ),
    )


class MVMetadataStatistics(Base):
    """Aggregate statistics for the landing page."""

    __tablename__ = "mv_metadata_statistics"

    field: Mapped[str] = mapped_column(Text, primary_key=True)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False)
