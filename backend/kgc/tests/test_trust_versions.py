"""Tests for the YAML version loader and canonical config hash.

The hash is the dedup signal that lets re-running an unchanged version be a
no-op while a single-character prompt edit forces a re-judge — so it must be
invariant under cosmetic yaml differences (key order, comments, whitespace)
but sensitive to any semantic content change.
"""

from __future__ import annotations

import textwrap
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from src.pipeline.trust.versions import (
    VersionBundle,
    compute_config_hash,
    load_version,
)

if TYPE_CHECKING:
    from pathlib import Path


def _write_yml(tmp_path: Path, signal_kind: str, version: str, body: str) -> Path:
    d = tmp_path / signal_kind
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{version}.yml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return p


_VALID_BODY = """\
    signal_kind: llm_plausibility
    description: test
    provider: gemini
    model: gemini-2.5-flash-lite
    generation:
      temperature: 0.0
      max_output_tokens: 256
      response_mime_type: application/json
    prompts:
      system: "you are a judge"
      user: "score this: {food}"
    response_schema:
      type: object
      required: [score, reason]
      properties:
        score: {type: number, minimum: 0, maximum: 1}
        reason: {type: string, maxLength: 280}
    """


class TestComputeConfigHash:
    def test_invariant_under_key_order(self):
        a = {"x": 1, "y": 2, "z": [3, 4]}
        b = {"z": [3, 4], "y": 2, "x": 1}
        assert compute_config_hash(a) == compute_config_hash(b)

    def test_changes_on_value_edit(self):
        a = {"prompt": "hello"}
        b = {"prompt": "hello!"}
        assert compute_config_hash(a) != compute_config_hash(b)

    def test_int_vs_float_distinct(self):
        # Documented quirk: yml authors should be consistent.
        assert compute_config_hash({"t": 0}) != compute_config_hash({"t": 0.0})

    def test_hex_length(self):
        h = compute_config_hash({"x": 1})
        assert len(h) == 64
        int(h, 16)  # raises if non-hex


class TestLoadVersion:
    def test_round_trip(self, tmp_path):
        _write_yml(tmp_path, "llm_plausibility", "v1", _VALID_BODY)
        bundle, h = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        assert isinstance(bundle, VersionBundle)
        assert bundle.provider == "gemini"
        assert bundle.model == "gemini-2.5-flash-lite"
        assert bundle.prompts.system == "you are a judge"
        assert len(h) == 64

    def test_cosmetic_edit_does_not_change_hash(self, tmp_path):
        _write_yml(tmp_path, "llm_plausibility", "v1", _VALID_BODY)
        _, h1 = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        # Reorder keys + add comment + extra blank line — same parsed structure.
        reordered = """\
            # a leading comment that should not affect the hash
            description: test
            signal_kind: llm_plausibility

            model: gemini-2.5-flash-lite
            provider: gemini
            generation:
              max_output_tokens: 256
              temperature: 0.0
              response_mime_type: application/json
            response_schema:
              type: object
              required: [score, reason]
              properties:
                score: {minimum: 0, type: number, maximum: 1}
                reason: {type: string, maxLength: 280}
            prompts:
              user: "score this: {food}"
              system: "you are a judge"
            """
        _write_yml(tmp_path, "llm_plausibility", "v1", reordered)
        _, h2 = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        assert h1 == h2

    def test_prompt_edit_changes_hash(self, tmp_path):
        _write_yml(tmp_path, "llm_plausibility", "v1", _VALID_BODY)
        _, h1 = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        edited = _VALID_BODY.replace("you are a judge", "you are a strict judge")
        _write_yml(tmp_path, "llm_plausibility", "v1", edited)
        _, h2 = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        assert h1 != h2

    def test_temperature_edit_changes_hash(self, tmp_path):
        _write_yml(tmp_path, "llm_plausibility", "v1", _VALID_BODY)
        _, h1 = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        edited = _VALID_BODY.replace("temperature: 0.0", "temperature: 0.7")
        _write_yml(tmp_path, "llm_plausibility", "v1", edited)
        _, h2 = load_version("llm_plausibility", "v1", base_dir=tmp_path)
        assert h1 != h2

    def test_signal_kind_directory_mismatch_rejected(self, tmp_path):
        # File says llm_plausibility but lives under range_check/ — reject.
        _write_yml(tmp_path, "range_check", "v1", _VALID_BODY)
        with pytest.raises(ValueError, match="signal_kind="):
            load_version("range_check", "v1", base_dir=tmp_path)

    def test_unknown_provider_rejected(self, tmp_path):
        bad = _VALID_BODY.replace("provider: gemini", "provider: vertex")
        _write_yml(tmp_path, "llm_plausibility", "v1", bad)
        with pytest.raises(ValidationError):
            load_version("llm_plausibility", "v1", base_dir=tmp_path)

    def test_missing_required_field_rejected(self, tmp_path):
        bad = _VALID_BODY.replace("    model: gemini-2.5-flash-lite\n", "")
        _write_yml(tmp_path, "llm_plausibility", "v1", bad)
        with pytest.raises(ValidationError):
            load_version("llm_plausibility", "v1", base_dir=tmp_path)
