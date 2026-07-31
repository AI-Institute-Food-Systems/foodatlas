"""Base food–chemical–bioactivity efficacy ORM model.

One row per (``foodatlas_id``, ``cid``, ``bioactivity_id``), loaded from KGC
``food_chemical_efficacy.parquet``. It places a food's dietary concentration of
a chemical (``food_conc_*``) against that chemical's dose-response curve for a
bioactivity (``logac50`` / ``hillslope`` / …) to estimate whether dietary levels
are pharmacologically meaningful (``dose_over_ac50_log`` / ``conc_vs_ac50`` /
``efficacy_response``).

Food, chemical, and bioactivity are all existing entities; the materializer
resolves ``cid`` → chemical and ``bioactivity_id`` (``E300…``) → concept into
``mv_food_chemical_efficacy``. ``bioactivity_id`` may be the literal
``"UNCLASSIFIED"``. Optional, like the measurement store: a KG built without the
bioactivity source has no such file and the loader skips the bulk-copy.
"""

from sqlalchemy import BigInteger, Boolean, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class BaseFoodChemicalEfficacy(Base):
    """One row per food × chemical × bioactivity efficacy estimate."""

    __tablename__ = "base_food_chemical_efficacy"

    foodatlas_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    cid: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    bioactivity_id: Mapped[str] = mapped_column(String(32), primary_key=True)

    food_name: Mapped[str] = mapped_column(Text, server_default="")

    # Dietary concentration of the chemical in the food.
    food_conc_mg_per_100g: Mapped[float | None] = mapped_column(Float, nullable=True)
    food_conc_mass_fraction_pct: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    conc_quality_flag: Mapped[str] = mapped_column(Text, server_default="")
    molecular_weight: Mapped[float | None] = mapped_column(Float, nullable=True)
    food_conc_m: Mapped[float | None] = mapped_column(Float, nullable=True)
    food_conc_logm: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Dose-response curve for the chemical × bioactivity.
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

    # Dietary dose vs. the active concentration (the efficacy estimate).
    dose_over_ac50_log: Mapped[float | None] = mapped_column(Float, nullable=True)
    conc_vs_ac50: Mapped[str] = mapped_column(Text, server_default="")
    efficacy_fraction: Mapped[float | None] = mapped_column(Float, nullable=True)
    efficacy_response: Mapped[float | None] = mapped_column(Float, nullable=True)
    saturated: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
