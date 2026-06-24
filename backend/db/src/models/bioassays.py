"""Base bioassay ORM model — the assay-level metadata dimension.

One row per ``source_assay_id`` (PubChem ``AID:`` / ChEMBL id), loaded from KGC
``bioassays.parquet``. The bioactivity materializer joins it onto each displayed
measurement (``assay_meta``); it is also queryable standalone for assay/target
lookups and the Phase-2 disease join (``target_entrez_gene`` is the CTD key).

Optional, like the measurement store: a KG built without the bioactivity source
has no ``bioassays.parquet`` and the loader skips the bulk-copy, leaving this
table empty.
"""

from sqlalchemy import Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseBioassay(Base):
    """One row per assay, with its target metadata."""

    __tablename__ = "base_bioassays"

    source_assay_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[str] = mapped_column(Text, server_default="")
    n_measurements: Mapped[int | None] = mapped_column(Integer, nullable=True)
    assay_description: Mapped[str] = mapped_column(Text, server_default="")
    target_id: Mapped[str] = mapped_column(Text, server_default="")
    target_name: Mapped[str] = mapped_column(Text, server_default="")
    target_organism: Mapped[str] = mapped_column(Text, server_default="")
    target_uniprot: Mapped[str] = mapped_column(Text, server_default="")
    target_entrez_gene: Mapped[str] = mapped_column(Text, server_default="")
    bioactivity_ids: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    last_modified: Mapped[str] = mapped_column(Text, server_default="")

    __table_args__ = (
        Index("ix_bioassays_entrez", "target_entrez_gene"),
        Index("ix_bioassays_uniprot", "target_uniprot"),
    )
