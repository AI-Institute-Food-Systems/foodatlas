"""S3 helpers for downloading parquet outputs to a local directory.

Supports the `s3://bucket/prefix` form used by the `db load --parquet-dir`
command. Keeps the local ETL logic untouched — callers download to a
temporary directory and then hand the path to `load_kg`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:  # pragma: no cover - import for type checkers only
    from pathlib import Path

    from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


def is_s3_uri(value: str) -> bool:
    """Return True if ``value`` looks like an ``s3://`` URI."""
    return value.startswith("s3://")


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/prefix`` URI into (bucket, prefix).

    The returned prefix has no leading slash and no trailing slash.
    """
    if not is_s3_uri(uri):
        msg = f"Not an s3:// URI: {uri}"
        raise ValueError(msg)
    parsed = urlparse(uri)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/").rstrip("/")
    if not bucket:
        msg = f"S3 URI missing bucket: {uri}"
        raise ValueError(msg)
    return bucket, prefix


def download_s3_prefix(
    uri: str,
    local_dir: Path,
    *,
    s3_client: S3Client | None = None,
) -> None:
    """Download every object under ``uri`` into ``local_dir``.

    Preserves the relative key layout below the prefix so a caller invoking
    ``download_s3_prefix("s3://bucket/kg", dest)`` gets the same file layout
    inside ``dest`` as the KGC pipeline produced locally.
    """
    bucket, prefix = parse_s3_uri(uri)
    local_dir.mkdir(parents=True, exist_ok=True)

    client = s3_client if s3_client is not None else _default_s3_client()

    logger.info("Downloading s3://%s/%s -> %s", bucket, prefix, local_dir)

    paginator = client.get_paginator("list_objects_v2")
    prefix_with_slash = f"{prefix}/" if prefix else ""
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_with_slash):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            relative = key[len(prefix_with_slash) :] if prefix_with_slash else key
            target = local_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket, key, str(target))
            count += 1

    if count == 0:
        msg = f"No objects found under {uri}"
        raise FileNotFoundError(msg)

    logger.info("Downloaded %d objects from s3://%s/%s", count, bucket, prefix)


def _default_s3_client() -> S3Client:
    """Create a default boto3 S3 client (deferred import)."""
    import boto3  # noqa: PLC0415 — deferred so tests can stub this module

    return boto3.client("s3")
