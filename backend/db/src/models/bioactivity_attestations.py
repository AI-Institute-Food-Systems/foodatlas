"""Bioactivity attestation ORM model — from KGC bioactivity_attestations.parquet.

Separate from BaseAttestation because of the ~12 columns specific to assay
metadata (potency, Hill coefficients, target ids) that don't apply to
FDC/CTD attestations. See docs/bioactivity-parquet-contract.md.
"""

from sqlalchemy import Boolean, Double, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseBioactivityAttestation(Base):
    """Attestation rows backing r5/r6/r7 triplets."""

    __tablename__ = "base_bioactivity_attestations"

    attestation_id: Mapped[str] = mapped_column(String(40), primary_key=True)
    evidence_id: Mapped[str] = mapped_column(
        String(30),
        ForeignKey("base_evidence.evidence_id"),
        nullable=False,
    )
    bioactivity_metadata_id: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    head_name_raw: Mapped[str] = mapped_column(Text, server_default="")
    tail_name_raw: Mapped[str] = mapped_column(Text, server_default="")
    head_candidates: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    tail_candidates: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    validated: Mapped[bool] = mapped_column(Boolean, server_default="false")
    validated_correct: Mapped[bool] = mapped_column(Boolean, server_default="true")

    source_assay_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_ids: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")

    evidence_value_potency_value: Mapped[float | None] = mapped_column(
        Double, nullable=True
    )
    evidence_value_potency_unit: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_value_efficacy_zeroactivity: Mapped[float | None] = mapped_column(
        Double, nullable=True
    )
    evidence_value_efficacy_infiniteactivity: Mapped[float | None] = mapped_column(
        Double, nullable=True
    )
    evidence_value_efficacy_logac50_value: Mapped[float | None] = mapped_column(
        Double, nullable=True
    )
    evidence_value_efficacy_hillslope: Mapped[float | None] = mapped_column(
        Double, nullable=True
    )

    evidence_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_type: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Populated only for r6 (food exhibits bioactivity): "direct" or "inherited".
    exhibit_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    # Populated only for r7 (bioactivity associated_with disease): improves/degrades.
    # Reserved column; sample CSV emits only "associated_with" so this ships NULL.
    polarity: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Populated only for inherited r6 attestations.
    derived_from_attestation_id: Mapped[str | None] = mapped_column(
        String(40), nullable=True
    )
    via_chemical_id: Mapped[str | None] = mapped_column(String(20), nullable=True)

    __table_args__ = (
        Index("ix_base_bio_att_evidence", "evidence_id"),
        Index("ix_base_bio_att_metadata", "bioactivity_metadata_id"),
        Index("ix_base_bio_att_exhibit", "exhibit_type"),
        Index("ix_base_bio_att_via_chem", "via_chemical_id"),
    )
