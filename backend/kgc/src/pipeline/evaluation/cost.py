"""Token-usage accounting and Claude pricing for the evaluation judge.

Prices are Claude Opus 4.8 list price (USD per 1M tokens) as of the prompt
version; update ``_PRICE`` if the judge model or pricing changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_PRICE = {
    "input": 5.0,
    "output": 25.0,
    "cache_write": 6.25,  # 1.25x input (5-minute TTL)
    "cache_read": 0.5,  # 0.1x input
}


@dataclass(frozen=True)
class Usage:
    """Token counts and API-call count for one or more judge requests."""

    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    def __add__(self, other: Usage) -> Usage:
        return Usage(
            self.calls + other.calls,
            self.input_tokens + other.input_tokens,
            self.output_tokens + other.output_tokens,
            self.cache_read_tokens + other.cache_read_tokens,
            self.cache_write_tokens + other.cache_write_tokens,
        )

    @property
    def cost_usd(self) -> float:
        return (
            self.input_tokens * _PRICE["input"]
            + self.output_tokens * _PRICE["output"]
            + self.cache_read_tokens * _PRICE["cache_read"]
            + self.cache_write_tokens * _PRICE["cache_write"]
        ) / 1_000_000


def usage_from_response(response: Any) -> Usage:
    """Read token counts off an Anthropic Messages response."""
    u = response.usage
    return Usage(
        calls=1,
        input_tokens=u.input_tokens or 0,
        output_tokens=u.output_tokens or 0,
        cache_read_tokens=getattr(u, "cache_read_input_tokens", 0) or 0,
        cache_write_tokens=getattr(u, "cache_creation_input_tokens", 0) or 0,
    )
