"""Search PubMed/PMC for food-chemical articles and fetch the missing BioC files.

Writes query_uid_results.tsv (all (PMID, PMCID) matches), missing_pmcids.txt
(PMCIDs not yet on disk), and fetch.log (progress of the on-demand fetch).
Sentence extraction happens in the separate retrieval stage.
"""

from __future__ import annotations

import logging
import os
import re
import time
from pathlib import Path

import pandas as pd

from .bioc_fetch import fetch_missing
from .pubmed_search import (
    get_pmcid_pmid_mapping,
    load_data,
    parse_query,
    search_queries,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_DATE_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}$")


def _latest_run_date(output_base_dir: str, current_date: str) -> str | None:
    """Most recent completed YYYY_MM_DD subdir, excluding *current_date*.
    Completion marker is query_uid_results.tsv (what this stage emits)."""
    base = Path(output_base_dir)
    if not base.is_dir():
        return None
    dates = sorted(
        d.name
        for d in base.iterdir()
        if d.is_dir()
        and _DATE_PATTERN.match(d.name)
        and d.name != current_date
        and (d / "query_uid_results.tsv").exists()
    )
    if not dates:
        return None
    return dates[-1].replace("_", "/")


def _unique_pmcids_from_tsv(tsv_path: Path) -> set[str]:
    df = pd.read_csv(tsv_path, sep="\t", dtype=str, keep_default_na=False)
    return {p for p in df["pmcid"].tolist() if p}


def _scan_cached_pmcids(bioc_dir: Path) -> set[str]:
    """One-pass scandir over bioc_dir collecting PMC*.xml stems. Avoids per-PMCID
    stat() calls — at millions of files in the cache, a single readdir is much
    cheaper than tens of thousands of individual existence checks."""
    present: set[str] = set()
    with os.scandir(bioc_dir) as it:
        for entry in it:
            name = entry.name
            if name.startswith("PMC") and name.endswith(".xml"):
                present.add(name[:-4])
    return present


def run_search(
    *,
    query: str,
    query_uid_results_filepath: str,
    filepath_bioc_pmc: str,
    output_base_dir: str,
    current_date: str,
    save_every: int = 50,
    min_date: str | None = None,
    fetch_workers: int = 16,
    fetch_timeout: float = 30.0,
) -> None:
    """Entrez search → set-diff against local BioC cache → fetch missing articles."""
    if min_date is None:
        min_date = _latest_run_date(output_base_dir, current_date)
        if min_date:
            log.info("Derived min_date from latest run folder: %s", min_date)

    pmcid_pmid_dict, pmid_pmcid_dict = get_pmcid_pmid_mapping()

    results_path = Path(query_uid_results_filepath)
    results_path.parent.mkdir(exist_ok=True, parents=True)
    bioc_path = Path(filepath_bioc_pmc)

    data, previous_queries = load_data(
        query_uid_results_filepath,
        pmcid_pmid_dict=pmcid_pmid_dict,
        pmid_pmcid_dict=pmid_pmcid_dict,
    )

    queries = parse_query(query)

    search_queries(
        queries=queries,
        data=data,
        previous_queries=previous_queries,
        pmcid_pmid_dict=pmcid_pmid_dict,
        pmid_pmcid_dict=pmid_pmcid_dict,
        email=os.environ.get("NCBI_EMAIL", "user@example.com"),
        min_date=min_date,
        save_every=save_every,
        save_filepath=query_uid_results_filepath,
        api_key=os.environ.get("NCBI_API_KEY"),
    )

    log.info("Computing missing PMCIDs ...")
    wanted = _unique_pmcids_from_tsv(results_path)
    t0 = time.monotonic()
    cached = _scan_cached_pmcids(bioc_path)
    log.info(
        "  requested=%d  cached=%d  (scan %.1fs)",
        len(wanted), len(cached), time.monotonic() - t0,
    )
    missing = sorted(wanted - cached)

    run_dir = results_path.parent
    missing_path = run_dir / "missing_pmcids.txt"
    missing_path.write_text("\n".join(missing) + ("\n" if missing else ""))
    log.info("  missing=%d  -> %s", len(missing), missing_path)

    if not missing:
        log.info("Nothing to fetch.")
        return

    email = os.environ.get("NCBI_EMAIL", "user@example.com")
    user_agent = f"foodatlas-ie/0.1 ({email})"
    fetch_log_path = run_dir / "fetch.log"
    log.info("Fetching %d articles (workers=%d, log=%s)",
             len(missing), fetch_workers, fetch_log_path)

    result = fetch_missing(
        missing,
        bioc_path,
        max_workers=fetch_workers,
        timeout=fetch_timeout,
        user_agent=user_agent,
        log_path=fetch_log_path,
    )

    if result.errors:
        error_log = run_dir / "fetch_errors.txt"
        error_log.write_text(
            "\n".join(f"{p}\t{status}" for p, status in result.errors) + "\n"
        )
        log.warning("  %d errors -> %s", len(result.errors), error_log)

    log.info(
        "Fetch done: ok=%d cached=%d not_in_oa=%d errors=%d",
        result.fetched,
        result.cached,
        len(result.not_in_oa),
        len(result.errors),
    )
