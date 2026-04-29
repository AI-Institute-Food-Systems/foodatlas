"""Trust-signal models — pydantic schema for the KGC trust pipeline output.

The KGC trust stage produces one :class:`TrustSignal` per (attestation, signal
version) and writes them to ``kg_dir/trust_signals.parquet``. The DB loader
upserts those rows into ``base_trust_signals``.

`signal_id` is content-addressed:
``sha256(attestation_id + "|" + signal_kind + "|" + version + "|" + config_hash)``
where ``config_hash`` is the canonicalized hash of the version yml that produced
the score. Re-running an unchanged version is a no-op; editing the yml — even a
single prompt word — produces a new ``config_hash`` and writes new rows
alongside the old, enabling A/B comparison without losing history.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class TrustSignal(BaseModel):
    """One trust-signal row, as written to ``trust_signals.parquet``.

    ``score`` is in ``[0, 1]`` for valid judgments. ``-1`` is a sentinel for
    rows where the LLM/transport errored; those rows carry a non-empty
    ``error_text`` and are picked up for retry by the next trust-stage run.
    """

    signal_id: str
    attestation_id: str
    signal_kind: str
    version: str
    config_hash: str
    model: str
    score: float = Field(ge=-1.0, le=1.0)
    reason: str = ""
    error_text: str = ""
    created_at: datetime


class LLMPlausibilityResponse(BaseModel):
    """Validated JSON shape returned by an LLM judge for plausibility.

    Strict ``[0, 1]`` — the LLM never produces the ``-1`` error sentinel; that
    is set by the runner when it has to write a row for an errored request.

    The prompt asks for ≤ 200-char reasons but we accept up to 500 here so a
    minor overrun does not throw away the (still-valid) score. Treat this as
    soft-rejection: the prompt tightens, the validator forgives.
    """

    score: float = Field(ge=0.0, le=1.0)
    reason: str = Field(default="", max_length=500)
