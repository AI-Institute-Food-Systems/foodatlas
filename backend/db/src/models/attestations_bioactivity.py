"""Base bioactivity-measurement ORM model — from KGC attestations_bioactivity.parquet.

This is the typed *attestation store* for bioactivity edges: ``r5``/``r6``
triplets' ``attestation_ids`` resolve here (not to ``base_attestations``),
because the dose-response schema (potency / efficacy / endpoint) has no overlap
with the food-chemical attestation schema. Self-evidencing — no FK, no separate
evidence row (the measurement is its own provenance, keyed by ``source_assay_id``).

Lives on :class:`Base`, so it is dropped/recreated on each ``db load`` like the
other base tables. The loader skips it gracefully when a KG was built without
the bioactivity source.
"""

from sqlalchemy import Double, Index, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseAttestationBioactivity(Base):
    """One row per ``bm…`` measurement (compound × assay → outcome/potency)."""

    __tablename__ = "base_attestations_bioactivity"

    bioactivity_metadata_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    exhibit_type: Mapped[str] = mapped_column(Text, server_default="")
    source_assay_id: Mapped[str] = mapped_column(Text, server_default="")
    reported_activity_outcome: Mapped[str] = mapped_column(Text, server_default="")
    evidence_endpoint_type: Mapped[str] = mapped_column(Text, server_default="")
    evidence_relation: Mapped[str] = mapped_column(Text, server_default="")
    potency_value: Mapped[float | None] = mapped_column(Double, nullable=True)
    potency_unit: Mapped[str] = mapped_column(Text, server_default="")
    efficacy_zeroactivity: Mapped[float | None] = mapped_column(Double, nullable=True)
    efficacy_infiniteactivity: Mapped[float | None] = mapped_column(Double, nullable=True)
    efficacy_logac50_value: Mapped[float | None] = mapped_column(Double, nullable=True)
    efficacy_hillslope: Mapped[float | None] = mapped_column(Double, nullable=True)
    evidence_source: Mapped[str] = mapped_column(Text, server_default="")
    evidence_type: Mapped[str] = mapped_column(Text, server_default="")
    evidence_fit_r2: Mapped[float | None] = mapped_column(Double, nullable=True)
    evidence_fit_curveclass: Mapped[str] = mapped_column(Text, server_default="")

    __table_args__ = (
        Index("ix_bab_assay", "source_assay_id"),
        Index("ix_bab_outcome", "reported_activity_outcome"),
        Index("ix_bab_endpoint", "evidence_endpoint_type"),
    )
