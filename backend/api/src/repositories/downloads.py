"""Downloads repository: read the public bundle manifest from S3.

The API does not hold credentials for the downloads bucket. The bucket
grants anonymous ``s3:GetObject`` (see :mod:`stacks.downloads_stack`),
so we fetch ``bundles/index.json`` over plain HTTPS. A missing or
malformed manifest yields an empty list so the downloads page renders
gracefully while there are no published bundles.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

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
