"""Base evidence ORM model — normalized, from KGC evidence.parquet."""

from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseEvidence(Base):
    """Immutable evidence records from KGC output."""

    __tablename__ = "base_evidence"

    evidence_id: Mapped[str] = mapped_column(String(30), primary_key=True)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    reference: Mapped[dict] = mapped_column(JSONB, nullable=False)

    __table_args__ = (Index("ix_base_evidence_source_type", "source_type"),)
