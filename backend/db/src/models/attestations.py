"""Base attestation ORM model — from KGC attestations.parquet."""

from sqlalchemy import Boolean, Double, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseAttestation(Base):
    """Attestation records linking evidence to triplets."""

    __tablename__ = "base_attestations"

    attestation_id: Mapped[str] = mapped_column(String(30), primary_key=True)
    evidence_id: Mapped[str] = mapped_column(
        String(30),
        ForeignKey("base_evidence.evidence_id"),
        nullable=False,
    )
    source: Mapped[str] = mapped_column(Text, nullable=False)
    head_name_raw: Mapped[str] = mapped_column(Text, server_default="")
    tail_name_raw: Mapped[str] = mapped_column(Text, server_default="")
    conc_value: Mapped[float | None] = mapped_column(Double, nullable=True)
    conc_unit: Mapped[str] = mapped_column(Text, server_default="")
    conc_value_raw: Mapped[str] = mapped_column(Text, server_default="")
    conc_unit_raw: Mapped[str] = mapped_column(Text, server_default="")
    food_part: Mapped[str] = mapped_column(Text, server_default="")
    food_processing: Mapped[str] = mapped_column(Text, server_default="")
    filter_score: Mapped[float | None] = mapped_column(Double, nullable=True)
    validated: Mapped[bool] = mapped_column(Boolean, server_default="false")
    validated_correct: Mapped[bool] = mapped_column(Boolean, server_default="true")
    head_candidates: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    tail_candidates: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")

    __table_args__ = (
        Index("ix_base_attestations_evidence", "evidence_id"),
        Index("ix_base_attestations_source", "source"),
    )
