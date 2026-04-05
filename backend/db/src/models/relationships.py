"""Relationship ORM model — from KGC relationships.parquet."""

from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Relationship(Base):
    """Relationship type definitions (r1-r5)."""

    __tablename__ = "relationships"

    foodatlas_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
