"""Base trust-signal ORM model — from KGC trust_signals.parquet."""

from datetime import datetime

from sqlalchemy import DateTime, Double, Index, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .trust_base import TrustBase


class BaseTrustSignal(TrustBase):
    """One score from one signal version for one attestation.

    `signal_id` is `sha256(attestation_id + signal_kind + version + config_hash)` so
    re-running an unchanged version is a no-op (handled via upsert in the loader).

    No FK to `base_attestations` — `attestation_id` is content-addressed and
    stable across KG rebuilds, but `base_attestations` is dropped/recreated on
    every `db load`. Skipping the FK lets trust signals outlive base rebuilds.
    """

    __tablename__ = "base_trust_signals"

    signal_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    attestation_id: Mapped[str] = mapped_column(String(30), nullable=False)
    signal_kind: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    # score in [0, 1] for valid judgments; -1 sentinel marks an LLM/transport
    # error and is paired with non-empty `error_text`. Rows with score < 0 are
    # selected for retry on the next trust-stage run.
    score: Mapped[float] = mapped_column(Double, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    error_text: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_base_trust_signals_attestation", "attestation_id"),
        Index(
            "ix_base_trust_signals_run",
            "signal_kind",
            "version",
            "config_hash",
        ),
        Index("ix_base_trust_signals_kind_score", "signal_kind", "score"),
    )
