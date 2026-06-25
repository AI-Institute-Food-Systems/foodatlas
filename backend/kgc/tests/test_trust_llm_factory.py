"""Tests for the trust/llm factory."""

from __future__ import annotations

import pytest
from src.pipeline.trust.llm import create_client


def test_create_client_unknown_provider_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unknown trust LLM provider"):
        create_client("bogus-vendor")
