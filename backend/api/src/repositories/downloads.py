"""Downloads repository: the bundle manifest and pre-signed bundle URLs.

The manifest (``bundles/index.json``) and the per-version ``SUMMARY.md``
stay anonymously readable (see :mod:`stacks.downloads_stack`), so the
manifest is fetched over plain HTTPS and a missing or malformed one yields
an empty list — the downloads page then renders an empty table.

The zips themselves are private. ``download_link`` in the manifest is still
the object's S3 URL, but that URL only works once signed: the API's task
role has ``s3:GetObject`` and :func:`presign_bundle` turns the manifest
link into a short-lived pre-signed URL, which is what
``/v1/bundles/{version}/download`` redirects a key holder to.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

MANIFEST_KEY = "bundles/index.json"
MANIFEST_TIMEOUT_SECONDS = 5.0


def manifest_url(bucket: str, region: str) -> str:
    """Build the public HTTPS URL for the bundle manifest."""
    return f"https://{bucket}.s3.{region}.amazonaws.com/{MANIFEST_KEY}"


async def fetch_manifest(
    bucket: str,
    region: str,
    *,
    timeout: float = MANIFEST_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Fetch the bundle manifest, returning ``[]`` on any failure."""
    url = manifest_url(bucket, region)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()
        if not isinstance(payload, list):
            msg = "Bundle manifest must be a JSON array"
            raise ValueError(msg)
    except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("Failed to fetch bundle manifest from %s: %s", url, exc)
        return []
    return payload


def object_key_from_url(url: str) -> str:
    """S3 object key from a manifest link (virtual-hosted or path-style)."""
    parsed = urlparse(url)
    path = unquote(parsed.path).lstrip("/")
    host = parsed.netloc
    # Path-style: https://s3.<region>.amazonaws.com/<bucket>/<key>
    if host.startswith("s3.") or host == "s3.amazonaws.com":
        return path.split("/", 1)[1] if "/" in path else ""
    return path


def normalise_version(version: str) -> str:
    """``v4.12`` and ``4.12`` name the same bundle."""
    return version.lstrip("vV")


def find_bundle(entries: list[dict[str, Any]], version: str) -> dict[str, Any] | None:
    wanted = normalise_version(version)
    return next(
        (e for e in entries if normalise_version(str(e.get("version", ""))) == wanted),
        None,
    )


def presign_bundle(
    entry: dict[str, Any],
    *,
    bucket: str,
    region: str,
    expires_seconds: int,
    client_factory: Callable[[], Any] | None = None,
) -> str:
    """Pre-signed GET URL for one manifest entry's zip.

    Signing is local arithmetic on the task role's credentials — no S3
    round-trip — so this is safe to call on the request path.
    ``client_factory`` is the test seam, like ``PublicKeyStore._client_factory``.
    """
    if client_factory is not None:
        client = client_factory()
    else:
        import boto3  # noqa: PLC0415

        client = boto3.client("s3", region_name=region)
    key = object_key_from_url(str(entry["download_link"]))
    url: str = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )
    return url
