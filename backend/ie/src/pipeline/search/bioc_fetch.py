"""Fetch BioC-PMC articles on demand from the NCBI BioC OA REST API.

Saves each article as {out_dir}/{PMCID}.xml — JSON content with .xml extension,
matching the legacy bulk-download naming so existing BioC readers work unchanged.
Writes are atomic (tmp + rename) to survive interrupted runs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .bioc_progress import close_logger, consume_futures, start_logger
from .throttle import RateLimiter, backoff_seconds

if TYPE_CHECKING:
    from collections.abc import Iterable

BIOC_URL_TEMPLATE = (
    "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi"
    "/BioC_json/{pmcid}/unicode"
)

log = logging.getLogger(__name__)


@dataclass
class FetchResult:
    fetched: int = 0
    cached: int = 0
    not_in_oa: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)

    def record(self, pmcid: str, status: str) -> None:
        """Tally one _fetch_one outcome (see its docstring for the statuses)."""
        if status == "ok":
            self.fetched += 1
        elif status == "cached":
            self.cached += 1
        elif status == "not_in_oa":
            self.not_in_oa.append(pmcid)
        else:
            self.errors.append((pmcid, status))


def _make_session(
    total_retries: int,
    backoff_factor: float,
    user_agent: str,
) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=64,
        pool_maxsize=64,
    )
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": user_agent})
    return session


def _cleanup_stray_tmp(out_dir: Path) -> int:
    removed = 0
    with os.scandir(out_dir) as it:
        for entry in it:
            if entry.name.endswith(".xml.tmp"):
                Path(entry.path).unlink(missing_ok=True)
                removed += 1
    return removed


def _try_fetch(
    session: requests.Session,
    url: str,
    timeout: float,
) -> tuple[str, Any]:
    """One HTTP attempt, classified for the retry loop as (kind, value):
      ('ok', payload)   — JSON body (bytes), ready to write
      ('miss', '')      — article not in the OA subset (terminal)
      ('retry', detail) — transient throttle/network failure (back off, retry)
      ('fatal', detail) — malformed payload we can't use (terminal)

    Two NCBI quirks: non-OA articles return HTTP 200 with body '[Error]' rather
    than 404; a rate-limited service returns HTTP 200 with an HTML throttle page,
    so `invalid_json` is a throttle signal, not corruption.
    """
    try:
        resp = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        return "retry", type(exc).__name__
    if resp.status_code == 404:
        return "miss", ""
    if resp.status_code != 200:
        return "retry", f"http_{resp.status_code}"
    return _classify_body(resp.content)


def _classify_body(body: bytes) -> tuple[str, Any]:
    """Classify an HTTP-200 body — same (kind, value) contract as _try_fetch."""
    if body.lstrip().startswith(b"[Error]"):
        return "miss", ""
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return "retry", "invalid_json"
    if isinstance(parsed, list):
        if len(parsed) != 1:
            return "fatal", f"unexpected_list_len_{len(parsed)}"
        parsed = parsed[0]
    if not isinstance(parsed, dict):
        return "fatal", f"unexpected_type_{type(parsed).__name__}"

    return "ok", json.dumps(parsed, ensure_ascii=False).encode("utf-8")


def _write_atomic(dest: Path, payload: bytes) -> None:
    tmp = dest.with_suffix(".xml.tmp")
    tmp.write_bytes(payload)
    tmp.rename(dest)


def _fetch_one(
    pmcid: str,
    out_dir: Path,
    session: requests.Session,
    timeout: float,
    limiter: RateLimiter,
    attempts: int,
) -> tuple[str, str]:
    """Fetch one article with backoff retries -> (pmcid, status):
    'ok' | 'cached' | 'not_in_oa' | 'error:<detail>'."""
    dest = out_dir / f"{pmcid}.xml"
    if dest.exists():
        return pmcid, "cached"

    url = BIOC_URL_TEMPLATE.format(pmcid=pmcid)
    detail = "unknown"
    for attempt in range(attempts):
        limiter.wait()
        kind, value = _try_fetch(session, url, timeout)
        if kind == "ok":
            _write_atomic(dest, value)
            return pmcid, "ok"
        if kind == "miss":
            return pmcid, "not_in_oa"
        if kind == "fatal":
            return pmcid, f"error:{value}"
        detail = value
        if attempt < attempts - 1:
            time.sleep(backoff_seconds(attempt))

    return pmcid, f"error:{detail}"


def fetch_missing(
    pmcids: Iterable[str],
    out_dir: Path,
    max_workers: int = 6,
    timeout: float = 30.0,
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    min_interval: float = 0.2,
    transient_attempts: int = 4,
    user_agent: str = "foodatlas-pmc-fetch/0.1",
    log_path: Path | None = None,
    log_interval_seconds: float = 5.0,
) -> FetchResult:
    """Fetch PMCIDs concurrently, paced to stay under NCBI's per-IP throttle
    and retrying transient throttle/network failures. If log_path is set, emits
    a start line, a progress line every log_interval_seconds, and a done line."""
    out_dir.mkdir(parents=True, exist_ok=True)
    removed = _cleanup_stray_tmp(out_dir)
    if removed:
        log.info("Cleaned up %d stray .xml.tmp files", removed)

    pmcid_list = list(pmcids)
    result = FetchResult()
    if not pmcid_list:
        return result

    session = _make_session(total_retries, backoff_factor, user_agent)
    limiter = RateLimiter(min_interval)
    progress_logger, progress_handler = start_logger(
        log_path, len(pmcid_list), max_workers, out_dir
    )
    t_start = time.monotonic()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    _fetch_one,
                    p,
                    out_dir,
                    session,
                    timeout,
                    limiter,
                    transient_attempts,
                ): p
                for p in pmcid_list
            }
            consume_futures(
                futures,
                result,
                len(pmcid_list),
                progress_logger,
                log_interval_seconds,
                t_start,
            )
    finally:
        close_logger(
            progress_logger, progress_handler, result, len(pmcid_list), t_start
        )

    return result
