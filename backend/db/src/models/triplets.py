"""Base triplet ORM model — normalized, from KGC triplets.parquet."""

from sqlalchemy import ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseTriplet(Base):
    """Normalized triplet table loaded directly from KGC output.

    Note: KGC triplets.parquet has no explicit ID column; we use a
    surrogate ``triplet_id`` (auto-increment) for the PK, with a
    unique constraint on the composite natural key.
    """

    __tablename__ = "base_triplets"

    triplet_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    head_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("base_entities.foodatlas_id"), nullable=False
    )
    relationship_id: Mapped[str] = mapped_column(
        String(10), ForeignKey("relationships.foodatlas_id"), nullable=False
    )
    tail_id: Mapped[str] = mapped_column(
        String(20), ForeignKey("base_entities.foodatlas_id"), nullable=False
    )
    source: Mapped[str] = mapped_column(Text, server_default="")
    attestation_ids: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")

    __table_args__ = (
        UniqueConstraint("head_id", "relationship_id", "tail_id", name="uq_triplet"),
        Index("ix_base_triplets_head", "head_id"),
        Index("ix_base_triplets_tail", "tail_id"),
        Index("ix_base_triplets_rel", "relationship_id"),
        Index("ix_base_triplets_head_rel", "head_id", "relationship_id"),
        Index("ix_base_triplets_tail_rel", "tail_id", "relationship_id"),
    )
