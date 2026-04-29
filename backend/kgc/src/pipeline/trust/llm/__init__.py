"""LLM clients for trust signals; pluggable via VersionBundle.provider."""

from __future__ import annotations

from .base import TrustLLMClient, TrustLLMRequest, TrustLLMResponse
from .bedrock import BedrockClient
from .gemini import GeminiClient
from .openai import OpenAIClient

__all__ = [
    "BedrockClient",
    "GeminiClient",
    "OpenAIClient",
    "TrustLLMClient",
    "TrustLLMRequest",
    "TrustLLMResponse",
    "create_client",
]


def create_client(provider: str) -> TrustLLMClient:
    """Factory: ``provider`` value from a version yml → live client.

    Raises ``ValueError`` on unknown providers and ``NotImplementedError``
    for stubbed providers — both fail at runner startup before any LLM
    spend happens.
    """
    if provider == "gemini":
        return GeminiClient()
    if provider == "bedrock":
        return BedrockClient()
    if provider == "openai":
        return OpenAIClient()
    msg = f"Unknown trust LLM provider: {provider!r}"
    raise ValueError(msg)
