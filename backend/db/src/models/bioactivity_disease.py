"""Base bioactivity disease-bridge ORM models (chemical↔disease inference input).

Two staged reference tables loaded from KGC:

* ``base_bioactivity_disease`` — disease (MeSH) → assay (``source_assay_id``)
  links, with the CTD ``relationship`` type(s) and the ``bdm…`` metadata ids.
* ``base_bioactivity_disease_targets`` — each ``bdm…`` id → its target gene ids.

The materializer joins the disease→assay bridge to chemical measurements
(a chemical *active* in the same assay) to infer chemical↔disease associations
(``mv_chemical_disease_bioactivity``). Optional: present only when the KG was
built with the bioactivity source.
"""

from sqlalchemy import BigInteger, String, Text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseBioactivityDisease(Base):
    """One row per disease↔assay link from the bioactivity-disease bridge."""

    __tablename__ = "base_bioactivity_disease"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    disease_mesh_id: Mapped[str] = mapped_column(String(32), server_default="")
    source_assay_id: Mapped[str] = mapped_column(String(64), server_default="")
    relationship: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
    bioactivity_disease_metadata_id: Mapped[list[str]] = mapped_column(
        ARRAY(Text), server_default="{}"
    )


class BaseBioactivityDiseaseTarget(Base):
    """One row per ``bdm…`` metadata id → its target gene ids."""

    __tablename__ = "base_bioactivity_disease_targets"

    bioactivity_disease_metadata_id: Mapped[str] = mapped_column(
        String(32), primary_key=True
    )
    target_ids: Mapped[list[str]] = mapped_column(ARRAY(Text), server_default="{}")
