"""Tests for the S3 download helper used by ``main.py load``."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from src.etl.s3_sync import (
    download_s3_prefix,
    is_s3_uri,
    parse_s3_uri,
)


def test_is_s3_uri_true() -> None:
    assert is_s3_uri("s3://bucket/prefix") is True


def test_is_s3_uri_false() -> None:
    assert is_s3_uri("/local/path") is False
    assert is_s3_uri("https://bucket.s3.amazonaws.com/prefix") is False


def test_parse_s3_uri_with_prefix() -> None:
    bucket, prefix = parse_s3_uri("s3://my-bucket/kg/outputs")
    assert bucket == "my-bucket"
    assert prefix == "kg/outputs"


def test_parse_s3_uri_without_prefix() -> None:
    bucket, prefix = parse_s3_uri("s3://my-bucket")
    assert bucket == "my-bucket"
    assert prefix == ""


def test_parse_s3_uri_strips_trailing_slash() -> None:
    bucket, prefix = parse_s3_uri("s3://my-bucket/kg/")
    assert bucket == "my-bucket"
    assert prefix == "kg"


def test_parse_s3_uri_rejects_non_s3() -> None:
    with pytest.raises(ValueError, match="Not an s3"):
        parse_s3_uri("/local/path")


def test_parse_s3_uri_rejects_missing_bucket() -> None:
    with pytest.raises(ValueError, match="missing bucket"):
        parse_s3_uri("s3:///just-a-prefix")


def _fake_s3_client(
    contents: list[dict[str, Any]],
) -> MagicMock:
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": contents}]
    client.get_paginator.return_value = paginator

    def _download(bucket: str, key: str, local_path: str) -> None:
        Path(local_path).write_text(f"{bucket}/{key}")

    client.download_file.side_effect = _download
    return client


def test_download_s3_prefix_writes_files(tmp_path: Path) -> None:
    client = _fake_s3_client(
        [
            {"Key": "kg/entities.parquet"},
            {"Key": "kg/triplets.parquet"},
            {"Key": "kg/intermediate/lookup.json"},
        ],
    )

    download_s3_prefix("s3://bucket/kg", tmp_path, s3_client=client)

    assert (tmp_path / "entities.parquet").read_text() == "bucket/kg/entities.parquet"
    assert (tmp_path / "triplets.parquet").read_text() == "bucket/kg/triplets.parquet"
    assert (
        tmp_path / "intermediate" / "lookup.json"
    ).read_text() == "bucket/kg/intermediate/lookup.json"


def test_download_s3_prefix_skips_directory_markers(tmp_path: Path) -> None:
    client = _fake_s3_client(
        [
            {"Key": "kg/"},
            {"Key": "kg/entities.parquet"},
        ],
    )

    download_s3_prefix("s3://bucket/kg", tmp_path, s3_client=client)

    assert (tmp_path / "entities.parquet").exists()
    assert client.download_file.call_count == 1


def test_download_s3_prefix_raises_when_empty(tmp_path: Path) -> None:
    client = _fake_s3_client([])
    with pytest.raises(FileNotFoundError, match="No objects"):
        download_s3_prefix("s3://bucket/missing", tmp_path, s3_client=client)
