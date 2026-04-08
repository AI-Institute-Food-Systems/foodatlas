"""Tests for extraction/run_extraction module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
from src.pipeline.extraction.run_extraction import (
    build_batch_jsonl,
    download_raw_results,
    load_prompt,
)

_SYSTEM = "You are an expert in food science and chemistry. "
_TEMP = 0.0
_MAX_TOKENS = 512


def test_load_prompt_v1():
    template = load_prompt("v1")
    assert isinstance(template, str)
    assert "{sentence}" in template


def test_build_batch_jsonl_single_row():
    df = pd.DataFrame({"sentence": ["Cocoa contains polyphenols."]})
    template = load_prompt("v1")
    result = build_batch_jsonl(
        df,
        "gpt-4",
        prompt_template=template,
        system_prompt=_SYSTEM,
        temperature=_TEMP,
        max_new_tokens=_MAX_TOKENS,
    )
    lines = result.decode().strip().split("\n")
    assert len(lines) == 1
    obj = json.loads(lines[0])
    assert obj["custom_id"] == "0"
    assert obj["body"]["model"] == "gpt-4"
    assert obj["body"]["temperature"] == 0.0
    assert "polyphenols" in obj["body"]["messages"][1]["content"]


def test_build_batch_jsonl_multiple_rows():
    df = pd.DataFrame({"sentence": ["First sentence.", "Second sentence."]})
    template = load_prompt("v1")
    result = build_batch_jsonl(
        df,
        "gpt-5",
        prompt_template=template,
        system_prompt=_SYSTEM,
        temperature=_TEMP,
        max_new_tokens=_MAX_TOKENS,
    )
    lines = result.decode().strip().split("\n")
    assert len(lines) == 2
    for i, line in enumerate(lines):
        obj = json.loads(line)
        assert obj["custom_id"] == str(i)


def test_download_raw_results(tmp_path):
    mock_client = MagicMock()
    mock_file_content = MagicMock()
    mock_file_content.text = '{"result": "test"}\n'
    mock_client.files.content.return_value = mock_file_content

    mock_batch = MagicMock()
    mock_batch.output_file_id = "file-123"

    save_path = str(tmp_path / "results.jsonl")
    download_raw_results(mock_client, mock_batch, save_path)

    content = Path(save_path).read_text()
    assert '{"result": "test"}' in content
