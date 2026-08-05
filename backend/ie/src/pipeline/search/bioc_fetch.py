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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

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


def _fetch_one(  # noqa: PLR0911 — branches map to distinct HTTP outcomes; refactor noise
    pmcid: str,
    out_dir: Path,
    session: requests.Session,
    timeout: float,
) -> tuple[str, str]:
    """Returns (pmcid, status) where status is 'ok' | 'cached' | 'not_in_oa'
    | 'error:<detail>'. Two NCBI quirks handled here: (1) non-OA articles
    return HTTP 200 with body '[Error] : ...' instead of 404; (2) the response
    is a single-element JSON array, which we unwrap to match legacy on-disk
    schema (a bare object)."""
    dest = out_dir / f"{pmcid}.xml"
    if dest.exists():
        return pmcid, "cached"

    url = BIOC_URL_TEMPLATE.format(pmcid=pmcid)
    try:
        resp = session.get(url, timeout=timeout)
    except requests.RequestException as exc:
        return pmcid, f"error:{type(exc).__name__}"

    if resp.status_code == 404:
        return pmcid, "not_in_oa"
    if resp.status_code != 200:
        return pmcid, f"error:http_{resp.status_code}"

    body = resp.content
    if body.lstrip().startswith(b"[Error]"):
        return pmcid, "not_in_oa"

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return pmcid, "error:invalid_json"

    if isinstance(parsed, list):
        if len(parsed) != 1:
            return pmcid, f"error:unexpected_list_len_{len(parsed)}"
        parsed = parsed[0]
    if not isinstance(parsed, dict):
        return pmcid, f"error:unexpected_type_{type(parsed).__name__}"

    payload = json.dumps(parsed, ensure_ascii=False).encode("utf-8")
    tmp = dest.with_suffix(".xml.tmp")
    tmp.write_bytes(payload)
    tmp.rename(dest)
    return pmcid, "ok"


def _make_progress_logger(
    log_path: Path,
) -> tuple[logging.Logger, logging.FileHandler]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S")
    )
    logger = logging.Logger(f"pmc_fetch.progress[{log_path.name}]")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger, handler


def _emit_progress(
    logger: logging.Logger,
    result: FetchResult,
    total: int,
    elapsed: float,
    tag: str = "progress",
) -> None:
    done = result.fetched + result.cached + len(result.not_in_oa) + len(result.errors)
    rate = done / elapsed if elapsed > 0 else 0.0
    logger.info(
        "%s %d/%d rate=%.1f/s ok=%d cached=%d oa_miss=%d err=%d",
        tag,
        done,
        total,
        rate,
        result.fetched,
        result.cached,
        len(result.not_in_oa),
        len(result.errors),
    )


def fetch_missing(  # noqa: PLR0912 — outcome-classification branches; refactor noise
    pmcids: Iterable[str],
    out_dir: Path,
    max_workers: int = 8,
    timeout: float = 30.0,
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    user_agent: str = "foodatlas-pmc-fetch/0.1",
    log_path: Path | None = None,
    log_interval_seconds: float = 5.0,
) -> FetchResult:
    """Fetch PMCIDs concurrently. If log_path is set, emits a start line,
    a progress line every log_interval_seconds, and a done line."""
    out_dir.mkdir(parents=True, exist_ok=True)
    removed = _cleanup_stray_tmp(out_dir)
    if removed:
        log.info("Cleaned up %d stray .xml.tmp files", removed)

    session = _make_session(total_retries, backoff_factor, user_agent)
    result = FetchResult()
    pmcid_list = list(pmcids)
    if not pmcid_list:
        return result

    progress_logger = None
    progress_handler = None
    if log_path is not None:
        progress_logger, progress_handler = _make_progress_logger(log_path)
        progress_logger.info(
            "start total=%d workers=%d out_dir=%s",
            len(pmcid_list),
            max_workers,
            out_dir,
        )

    t_start = time.monotonic()
    last_log_t = t_start

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(_fetch_one, p, out_dir, session, timeout): p
                for p in pmcid_list
            }
            pbar = tqdm(
                total=len(pmcid_list),
                unit="article",
                smoothing=0.05,
            )
            try:
                for fut in as_completed(futures):
                    pmcid, status = fut.result()
                    if status == "ok":
                        result.fetched += 1
                    elif status == "cached":
                        result.cached += 1
                    elif status == "not_in_oa":
                        result.not_in_oa.append(pmcid)
                    else:
                        result.errors.append((pmcid, status))
                    pbar.update(1)

                    now = time.monotonic()
                    if progress_logger and (now - last_log_t) >= log_interval_seconds:
                        _emit_progress(
                            progress_logger,
                            result,
                            len(pmcid_list),
                            now - t_start,
                        )
                        last_log_t = now

                    if (result.fetched + result.cached) % 200 == 0:
                        pbar.set_postfix_str(
                            f"ok={result.fetched} oa_miss={len(result.not_in_oa)}"
                            f" err={len(result.errors)}"
                        )
            finally:
                pbar.close()
    finally:
        if progress_logger is not None and progress_handler is not None:
            _emit_progress(
                progress_logger,
                result,
                len(pmcid_list),
                time.monotonic() - t_start,
                tag="done",
            )
            progress_handler.close()
            progress_logger.removeHandler(progress_handler)

    return result
