"""Tests for trust LLM clients.

Heavy LLM-batch flow (uploads, polling, downloads) is exercised via the
smoke test, not unit tests — mocking every SDK call is brittle and shallow.
Unit tests cover: factory dispatch, fail-fast on missing config, stub
behaviour, empty-input no-op.
"""

from __future__ import annotations

import pytest
from src.pipeline.trust.llm import (
    BedrockClient,
    GeminiClient,
    OpenAIClient,
    create_client,
)
from src.pipeline.trust.llm import gemini as gemini_module


class TestCreateClient:
    def test_unknown_provider(self):
        with pytest.raises(ValueError, match="Unknown trust LLM provider"):
            create_client("anthropic")

    def test_bedrock_stub_fails_fast(self):
        with pytest.raises(NotImplementedError):
            create_client("bedrock")

    def test_openai_stub_fails_fast(self):
        with pytest.raises(NotImplementedError):
            create_client("openai")


class TestStubsRaiseOnInit:
    def test_bedrock_init_raises(self):
        with pytest.raises(NotImplementedError, match="BedrockClient"):
            BedrockClient()

    def test_openai_init_raises(self):
        with pytest.raises(NotImplementedError, match="OpenAIClient"):
            OpenAIClient()


class TestGeminiClient:
    def test_missing_api_key_raises(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="GOOGLE_API_KEY"):
            GeminiClient()

    def test_empty_requests_is_noop(self, monkeypatch):
        # No SDK call required when there are no requests; constructor still
        # needs to succeed.
        monkeypatch.setenv("GOOGLE_API_KEY", "fake-key-for-test")

        class _FakeClient:
            def __init__(self, *_, **__):
                pass

        # Patch genai.Client so __init__ does not hit the real SDK.
        monkeypatch.setattr(gemini_module.genai, "Client", _FakeClient)

        client = GeminiClient(api_key="fake-key-for-test")
        bundle = object()  # not used on the empty path
        assert client.submit_batch(bundle, []) == []  # type: ignore[arg-type]
