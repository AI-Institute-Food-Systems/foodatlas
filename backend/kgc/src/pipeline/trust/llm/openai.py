"""OpenAI client stub. Not implemented for v1 — fail fast at config time."""

from __future__ import annotations

from .base import TrustLLMClient, TrustLLMRequest, TrustLLMResponse


class OpenAIClient(TrustLLMClient):
    """Stub. v1 ships Gemini only; switch via ``provider:`` in version yml."""

    def __init__(self) -> None:
        msg = (
            "OpenAIClient is not implemented yet. v1 ships with provider=gemini; "
            "set provider: gemini in the version yml or implement OpenAIClient."
        )
        raise NotImplementedError(msg)

    def submit_batch(  # pragma: no cover - unreachable; __init__ raises
        self,
        bundle,
        requests: list[TrustLLMRequest],
        *,
        batch_mode: bool = True,
    ) -> list[TrustLLMResponse]:
        raise NotImplementedError
