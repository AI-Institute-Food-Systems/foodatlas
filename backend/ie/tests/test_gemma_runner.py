"""Tests for the non-vLLM helpers in extraction/gemma/runner.

vLLM itself is lazy-imported inside _init_engine and _process_chunks and
is GPU-only, so we cover everything *around* it: prompt parsing, message
building, aggregation of chunk TSVs, env-PATH munging, and CLI parsing."""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING
from unittest.mock import patch

import pandas as pd
import pytest
from src.pipeline.extraction.gemma.runner import (
    _CONDA_ENV_BIN,
    _PARAGRAPH_MARKER,
    _aggregate,
    _build_messages,
    _ensure_path,
    _load_system_instructions,
    _parse_args,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestLoadSystemInstructions:
    def test_returns_text_before_paragraph_marker(self, tmp_path: Path) -> None:
        path = tmp_path / "prompt.txt"
        path.write_text(f"Be precise.\n{_PARAGRAPH_MARKER}\nignored body")
        assert _load_system_instructions(str(path)) == "Be precise."

    def test_returns_full_text_when_marker_missing(self, tmp_path: Path) -> None:
        path = tmp_path / "prompt.txt"
        path.write_text("  no marker here  ")
        assert _load_system_instructions(str(path)) == "no marker here"


class TestBuildMessages:
    def test_one_message_pair_per_sentence(self) -> None:
        msgs = _build_messages(["a", "b"], system_instructions="sys")
        assert len(msgs) == 2
        for pair in msgs:
            assert pair[0]["role"] == "system"
            assert pair[0]["content"] == "sys"
            assert pair[1]["role"] == "user"
        assert msgs[0][1]["content"] == "a"
        assert msgs[1][1]["content"] == "b"


class TestAggregate:
    def test_concatenates_chunks_and_renames_answer(self, tmp_path: Path) -> None:
        pd.DataFrame({"sentence": ["s1"], "answer": ["0.9"]}).to_csv(
            tmp_path / "chunk_0000000.tsv", sep="\t", index=False
        )
        pd.DataFrame({"sentence": ["s2"], "answer": ["0.8"]}).to_csv(
            tmp_path / "chunk_0000001.tsv", sep="\t", index=False
        )
        out = _aggregate(tmp_path)
        assert out == tmp_path / "extraction_predicted.tsv"
        df = pd.read_csv(out, sep="\t")
        assert "prob" in df.columns and "answer" not in df.columns
        assert df["sentence"].tolist() == ["s1", "s2"]

    def test_raises_when_no_chunks(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            _aggregate(tmp_path)

    def test_preserves_existing_prob_column(self, tmp_path: Path) -> None:
        pd.DataFrame({"sentence": ["s1"], "prob": ["0.7"]}).to_csv(
            tmp_path / "chunk_0000000.tsv", sep="\t", index=False
        )
        out = _aggregate(tmp_path)
        df = pd.read_csv(out, sep="\t")
        assert "prob" in df.columns


class TestEnsurePath:
    def test_prepends_conda_bin_to_path(self) -> None:
        with patch.dict(os.environ, {"PATH": "/usr/bin"}, clear=False):
            _ensure_path()
            assert os.environ["PATH"].startswith(_CONDA_ENV_BIN)

    def test_noop_when_already_in_path(self) -> None:
        with patch.dict(
            os.environ, {"PATH": f"{_CONDA_ENV_BIN}:/usr/bin"}, clear=False
        ):
            before = os.environ["PATH"]
            _ensure_path()
            assert os.environ["PATH"] == before


class TestParseArgs:
    def test_required_args(self) -> None:
        with patch.object(
            sys,
            "argv",
            ["runner.py", "--input_path", "in.tsv", "--output_dir", "out/"],
        ):
            ns = _parse_args()
        assert ns.input_path == "in.tsv"
        assert ns.output_dir == "out/"
        # Defaults flow through
        assert ns.max_new_tokens == 1024
        assert ns.temperature == 0.0
        assert ns.tensor_parallel_size == 4
