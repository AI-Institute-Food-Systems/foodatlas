"""Base entity ORM model — normalized, from KGC entities.parquet."""

from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseEntity(Base):
    """Normalized entity table loaded directly from KGC output."""

    __tablename__ = "base_entities"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    entity_type: Mapped[str] = mapped_column(String(20), nullable=False)
    common_name: Mapped[str] = mapped_column(Text, nullable=False)
    scientific_name: Mapped[str] = mapped_column(Text, server_default="")
    synonyms: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    external_ids: Mapped[dict] = mapped_column(JSONB, server_default="{}")

    __table_args__ = (
        Index("ix_base_entities_type", "entity_type"),
        Index("ix_base_entities_common_name", "common_name"),
        Index("ix_base_entities_type_name", "entity_type", "common_name"),
    )
