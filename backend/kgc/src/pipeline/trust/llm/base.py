"""Abstract LLM client for trust signals.

The runner builds one :class:`TrustLLMRequest` per attestation, hands the
collection to a provider-specific :class:`TrustLLMClient`, and writes one
``base_trust_signals`` row per :class:`TrustLLMResponse`. Clients are
responsible for batching, polling, and translating the version-bundle
generation params + response schema into the provider's call shape.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..versions import VersionBundle


@dataclass(frozen=True)
class TrustLLMRequest:
    """One judge request, identified by a stable key for response join."""

    key: str
    user_prompt: str


@dataclass(frozen=True)
class TrustLLMResponse:
    """One judge response. Exactly one of ``raw_text`` / ``error`` is set."""

    key: str
    raw_text: str | None
    error: str | None


class TrustLLMClient(ABC):
    """Provider-agnostic LLM client used by the trust pipeline."""

    @abstractmethod
    def submit_batch(
        self,
        bundle: VersionBundle,
        requests: list[TrustLLMRequest],
        *,
        batch_mode: bool = True,
    ) -> list[TrustLLMResponse]:
        """Run ``requests`` through the configured model, in input order.

        ``batch_mode`` is operational: ``True`` prefers the provider's Batch
        API (cheap, ~minutes-to-hours latency); ``False`` issues sync calls
        (full price, immediate). Implementations may ignore the flag if a
        provider doesn't support both modes.

        Returns one response per request; the i-th response corresponds to
        the i-th request, matched by ``key``. A row the provider failed to
        process should appear as a :class:`TrustLLMResponse` with
        ``raw_text=None`` and a non-empty ``error`` string — the runner
        records it as a ``score=-1`` row so a future re-run can retry.
        """
