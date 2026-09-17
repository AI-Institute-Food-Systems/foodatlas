"""Materialized API tables for the bioactivity analysis layer.

Split out of ``views.py`` to keep that module small. Truncated and repopulated
on each ETL run; the API queries only these tables, not the base tables.
"""

from sqlalchemy import BigInteger, Boolean, Float, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class MVFoodChemicalEfficacy(Base):
    """Denormalized foodxchemicalxbioactivity efficacy, resolved to entities.

    One row per (food, chemical, bioactivity) from base_food_chemical_efficacy,
    with the chemical (by ``cid``) and bioactivity concept (by ``E300…`` native
    id) resolved to their entity names/ids. ``bioactivity_*`` are empty when the
    source row is ``UNCLASSIFIED``.
    """

    __tablename__ = "mv_food_chemical_efficacy"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    food_name: Mapped[str] = mapped_column(Text, nullable=False)
    food_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    chemical_name: Mapped[str] = mapped_column(Text, nullable=False)
    chemical_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    cid: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bioactivity_name: Mapped[str] = mapped_column(Text, server_default="")
    bioactivity_foodatlas_id: Mapped[str] = mapped_column(String(20), server_default="")
    bioactivity_id_raw: Mapped[str] = mapped_column(String(32), server_default="")

    food_conc_mg_per_100g: Mapped[float | None] = mapped_column(Float, nullable=True)
    food_conc_mass_fraction_pct: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    conc_quality_flag: Mapped[str] = mapped_column(Text, server_default="")
    molecular_weight: Mapped[float | None] = mapped_column(Float, nullable=True)
    food_conc_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    food_conc_logm: Mapped[float | None] = mapped_column(Float, nullable=True)
    rep_source_assay_id: Mapped[str] = mapped_column(Text, server_default="")
    endpoint_type: Mapped[str] = mapped_column(Text, server_default="")
    endpoint_class: Mapped[str] = mapped_column(Text, server_default="")
    curve_method: Mapped[str] = mapped_column(Text, server_default="")
    logac50: Mapped[float | None] = mapped_column(Float, nullable=True)
    hillslope: Mapped[float | None] = mapped_column(Float, nullable=True)
    zeroactivity: Mapped[float | None] = mapped_column(Float, nullable=True)
    infiniteactivity: Mapped[float | None] = mapped_column(Float, nullable=True)
    n_curves: Mapped[int | None] = mapped_column(Integer, nullable=True)
    n_curves_4param: Mapped[int | None] = mapped_column(Integer, nullable=True)
    curve_agreement: Mapped[str] = mapped_column(Text, server_default="")
    ac50_spread_log: Mapped[float | None] = mapped_column(Float, nullable=True)
    logac50_median: Mapped[float | None] = mapped_column(Float, nullable=True)
    logac50_min: Mapped[float | None] = mapped_column(Float, nullable=True)
    logac50_max: Mapped[float | None] = mapped_column(Float, nullable=True)
    dose_over_ac50_log: Mapped[float | None] = mapped_column(Float, nullable=True)
    conc_vs_ac50: Mapped[str] = mapped_column(Text, server_default="")
    efficacy_fraction: Mapped[float | None] = mapped_column(Float, nullable=True)
    efficacy_response: Mapped[float | None] = mapped_column(Float, nullable=True)
    saturated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    __table_args__ = (
        Index("ix_mv_fce_food", "food_name"),
        Index("ix_mv_fce_chemical", "chemical_name"),
        Index("ix_mv_fce_bioactivity", "bioactivity_name"),
    )


class MVChemicalDiseaseBioactivity(Base):
    """Chemical↔disease associations inferred from shared bioactivity assays.

    One row per (chemical, disease): the chemical has ≥1 *active* measurement in
    an assay that the bioactivity-disease bridge ties to the disease. Distinct
    from ``mv_chemical_disease_correlation`` (CTD literature correlations).
    """

    __tablename__ = "mv_chemical_disease_bioactivity"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    chemical_name: Mapped[str] = mapped_column(Text, nullable=False)
    chemical_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    disease_name: Mapped[str] = mapped_column(Text, nullable=False)
    disease_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    n_assays: Mapped[int] = mapped_column(Integer, server_default="0")
    n_active_measurements: Mapped[int] = mapped_column(Integer, server_default="0")
    relationships: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    target_genes: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    assays: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    # Same vocabulary as ``relationships``, but sourced from CTD *literature*
    # (mv_chemical_disease_correlation) rather than from the assay bridge —
    # so the two can be compared. Empty for ~97.5% of rows; that rarity is
    # what makes a match worth showing.
    literature_directions: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )

    __table_args__ = (
        Index("ix_mv_cdb_chemical", "chemical_name"),
        Index("ix_mv_cdb_disease", "disease_name"),
    )


class MVDiseaseBioactivity(Base):
    """Disease↔bioactivity↔chemical, attributed at the *assay* level.

    One row per (disease, bioactivity, chemical). Built from the same evidence
    as ``mv_chemical_disease_bioactivity``, but keeps the bioactivity of the
    bridging assay instead of collapsing it away.

    That distinction is the whole point. Going disease → chemical → *all* of
    that chemical's bioactivities credits a disease with every activity its
    chemicals happen to have — melanoma comes out with 1,571 "antiviral"
    chemicals. Attributing through the assay that actually bridges to the
    disease gives 4. The loose path measures how many chemicals a disease has;
    this one measures which activities characterise it.
    """

    __tablename__ = "mv_disease_bioactivity"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    disease_name: Mapped[str] = mapped_column(Text, nullable=False)
    disease_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    bioactivity_name: Mapped[str] = mapped_column(Text, nullable=False)
    bioactivity_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    chemical_name: Mapped[str] = mapped_column(Text, nullable=False)
    chemical_foodatlas_id: Mapped[str] = mapped_column(String(20), nullable=False)
    n_assays: Mapped[int] = mapped_column(Integer, server_default="0")
    n_active_measurements: Mapped[int] = mapped_column(Integer, server_default="0")
    relationships: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    target_genes: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    assays: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    literature_directions: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )

    __table_args__ = (
        Index("ix_mv_db_disease", "disease_name"),
        Index("ix_mv_db_disease_bio", "disease_name", "bioactivity_name"),
        Index("ix_mv_db_chemical", "chemical_name"),
        Index("ix_mv_db_bioactivity", "bioactivity_name"),
    )


class MVAssayTargetLabel(Base):
    """Gene id → human-readable protein name, for labelling assay targets.

    ``base_bioassays`` stores a free-text ``target_name`` per assay, so the same
    gene picks up several spellings ("Nuclear factor erythroid 2-related factor
    2", "Keap1/Nrf2", "…isoform 1 [Homo sapiens]"). This view keeps the *modal*
    name per gene id, which also collapses the Entrez/UniProt split — both
    ``NCBIGene: 4780`` and ``UniProt: Q16236`` resolve to the same label.

    A lookup table rather than a ``target_labels[]`` column on the big views:
    parallel arrays have to stay index-aligned with ``target_genes`` forever,
    and ~6k rows joined on demand costs nothing.
    """

    __tablename__ = "mv_assay_target_labels"

    # Prefixed id exactly as it appears in ``target_genes`` — "NCBIGene: 4780"
    # or "UniProt: Q16236" — so the join needs no string surgery.
    gene_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    label: Mapped[str] = mapped_column(Text, server_default="")
