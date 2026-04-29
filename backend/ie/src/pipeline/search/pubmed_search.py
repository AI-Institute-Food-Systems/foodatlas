"""PubMed/PMC search utilities: query parsing, ID mapping, and E-utility calls."""

from __future__ import annotations

import logging
import threading
import time
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

import pandas as pd
from Bio import Entrez
from tqdm import tqdm

log = logging.getLogger(__name__)

QUERY_TEMPLATE = "{food} AND ((compound) OR (nutrient))"


def parse_query(query: str) -> list[str]:
    """Parse a query string or .txt file into a list of search terms."""
    if query.endswith(".txt"):
        df = pd.read_csv(query, sep="\t", keep_default_na=False)
        queries: list[str] = df["query"].tolist()
    elif "," in query:
        queries = query.split(",")
    else:
        queries = [query]

    log.info("Got %d queries. First five: %s", len(queries), queries[:5])
    return queries


def get_pmcid_pmid_mapping(
    filepath: str = "outputs/corpus/NCBI/PMC-ids.csv",
) -> tuple[dict[str, str], dict[str, str]]:
    """Load PMCID<->PMID mapping from the NCBI CSV file."""
    log.info("Fetching PMCID-PMID mapping...")
    df = pd.read_csv(
        filepath,
        usecols=["PMCID", "PMID"],
        dtype=str,
        keep_default_na=False,
    )
    pmcid_pmid_dict: dict[str, str] = dict(zip(df["PMCID"], df["PMID"], strict=True))
    pmid_pmcid_dict: dict[str, str] = dict(zip(df["PMID"], df["PMCID"], strict=True))
    return pmcid_pmid_dict, pmid_pmcid_dict


def load_data(
    filepath: str,
    pmcid_pmid_dict: dict[str, str],
    pmid_pmcid_dict: dict[str, str],
) -> tuple[dict[tuple[str, str], list[str]], set[str]]:
    """Load previous query-UID results from a TSV file."""
    log.info("Loading data from %s...", filepath)

    if not Path(filepath).is_file():
        return {}, set()

    df = pd.read_csv(filepath, sep="\t", dtype=str, keep_default_na=False)

    log.info("Checking pmid & pmcid integrity...")
    has_both = (df["pmid"] != "") & (df["pmcid"] != "")
    if has_both.any():
        expected_pmcid = df.loc[has_both, "pmid"].map(pmid_pmcid_dict)
        mismatched = expected_pmcid != df.loc[has_both, "pmcid"]
        if mismatched.any():
            bad = df.loc[has_both].loc[mismatched].iloc[0]
            msg = f"PMCID mismatch for PMID {bad['pmid']}"
            raise ValueError(msg)

        expected_pmid = df.loc[has_both, "pmcid"].map(pmcid_pmid_dict)
        mismatched = expected_pmid != df.loc[has_both, "pmid"]
        if mismatched.any():
            bad = df.loc[has_both].loc[mismatched].iloc[0]
            msg = f"PMID mismatch for PMCID {bad['pmcid']}"
            raise ValueError(msg)

    pmid_empty = (df["pmid"] == "") & (df["pmcid"] != "")
    if pmid_empty.any():
        filled = df.loc[pmid_empty, "pmcid"].map(pmcid_pmid_dict)
        df.loc[pmid_empty, "pmid"] = filled.where(filled.notna(), "")

    pmcid_empty = (df["pmcid"] == "") & (df["pmid"] != "")
    if pmcid_empty.any():
        filled = df.loc[pmcid_empty, "pmid"].map(pmid_pmcid_dict)
        df.loc[pmcid_empty, "pmcid"] = filled.where(filled.notna(), "")

    df["key"] = list(zip(df["pmid"], df["pmcid"], strict=True))
    df["queries"] = df["queries"].apply(literal_eval)

    data: dict[tuple[str, str], list[str]] = dict(
        zip(df["key"], df["queries"], strict=True)
    )
    previous_queries: set[str] = {y for x in data.values() for y in x}

    return data, previous_queries


def save_data(data: dict[tuple[str, str], list[Any]], filepath: str) -> None:
    """Save query-UID results to a TSV file."""
    df = pd.DataFrame({"key": list(data.keys()), "queries": list(data.values())})
    df["pmid"] = df["key"].apply(lambda x: x[0])
    df["pmcid"] = df["key"].apply(lambda x: x[1])
    df = df[["pmid", "pmcid", "queries"]]
    df.to_csv(filepath, sep="\t", index=False)


def _resolve_uid(
    uid: str,
    db: str,
    pmcid_pmid_dict: dict[str, str],
    pmid_pmcid_dict: dict[str, str],
) -> tuple[str, str]:
    """Resolve a UID to a (pmid, pmcid) pair."""
    if db == "pmc":
        pmid = pmcid_pmid_dict.get(uid, "")
        return pmid, uid
    pmcid = pmid_pmcid_dict.get(uid, "")
    return uid, pmcid


_ESEARCH_MAX = 9999
_MIN_DATE = "1900/01/01"
_MAX_DATE = "2030/12/31"

_api_calls = 0
_api_calls_lock = threading.Lock()
_rate_limiter: threading.Semaphore | None = None
_rate_min_interval = 0.0
_rate_last_call = 0.0


_MAX_RETRIES = 3
_RETRY_BACKOFF = 5.0


def _init_rate_limiter(api_key: str | None) -> None:
    """Initialize the global rate limiter for NCBI API calls."""
    global _rate_limiter, _rate_min_interval, _rate_last_call  # noqa: PLW0603
    # With API key: 10 req/s, without: 3 req/s.
    # Semaphore limits concurrent in-flight requests.
    # min_interval ensures spacing between requests.
    if api_key:
        _rate_limiter = threading.Semaphore(4)
        _rate_min_interval = 0.12
    else:
        _rate_limiter = threading.Semaphore(1)
        _rate_min_interval = 0.34
    _rate_last_call = 0.0


def _esearch(
    db: str,
    term: str,
    min_date: str,
    max_date: str,
    retmax: int,
) -> dict[str, Any]:
    global _api_calls, _rate_last_call  # noqa: PLW0603
    assert _rate_limiter is not None
    kwargs: dict[str, Any] = {
        "db": db,
        "term": term,
        "datetype": "pdat",
        "mindate": min_date,
        "maxdate": max_date,
        "retmax": retmax,
    }
    for attempt in range(_MAX_RETRIES):
        with _rate_limiter:
            with _api_calls_lock:
                elapsed = time.monotonic() - _rate_last_call
                if elapsed < _rate_min_interval:
                    time.sleep(_rate_min_interval - elapsed)
                _rate_last_call = time.monotonic()
            try:
                with Entrez.esearch(**kwargs) as handle:
                    result: dict[str, Any] = Entrez.read(handle)
                with _api_calls_lock:
                    _api_calls += 1
                return result
            except Exception:
                if attempt == _MAX_RETRIES - 1:
                    raise
                wait = _RETRY_BACKOFF * (attempt + 1)
                log.warning("NCBI request failed, retrying in %.0fs...", wait)
                time.sleep(wait)
    msg = "Unreachable"
    raise RuntimeError(msg)


def _search_single_db(
    db: str,
    query: str,
    min_date: str | None,
) -> list[str]:
    """Search one NCBI database and return list of UIDs."""
    term = QUERY_TEMPLATE.format(food=query)
    uids = _collect_ids(db, term, min_date or _MIN_DATE, _MAX_DATE)
    return [f"PMC{uid}" if db == "pmc" else uid for uid in uids]


def _parse_date(date_str: str) -> date:
    parts = date_str.split("/")
    return date(int(parts[0]), int(parts[1]), int(parts[2]))


def _fmt_date(d: date) -> str:
    return f"{d.year}/{d.month:02d}/{d.day:02d}"


def _collect_ids(
    db: str,
    term: str,
    min_date: str,
    max_date: str,
) -> list[str]:
    """Collect all UIDs, splitting by date range if results exceed the cap."""
    result = _esearch(db, term, min_date, max_date, retmax=_ESEARCH_MAX)
    count = int(result["Count"])
    if count == 0:
        return []
    if count <= _ESEARCH_MAX:
        return list(result["IdList"])

    d_min = _parse_date(min_date)
    d_max = _parse_date(max_date)
    delta_days = (d_max - d_min).days
    if delta_days < 1:
        tqdm.write(
            f"[WARNING] {term!r} on {db}: {count:,} results on"
            f" {min_date}, capped at {_ESEARCH_MAX:,}"
        )
        return list(result["IdList"])

    d_mid = d_min + timedelta(days=delta_days // 2)
    left = _collect_ids(db, term, min_date, _fmt_date(d_mid))
    right = _collect_ids(
        db,
        term,
        _fmt_date(d_mid + timedelta(days=1)),
        max_date,
    )
    return left + right


def _search_both_dbs(
    q: str,
    min_date: str | None,
) -> tuple[bool, list[tuple[str, str, list[str]]]]:
    """Search pubmed and pmc for a query. Return (all_ok, results)."""
    results: list[tuple[str, str, list[str]]] = []
    succeeded = True
    for db in ("pubmed", "pmc"):
        try:
            ids = _search_single_db(db, q, min_date)
            results.append((q, db, ids))
        except Exception:
            log.exception("Exception while processing %s on %s", q, db)
            succeeded = False
    return succeeded, results


def _merge_results(
    results: list[tuple[str, str, list[str]]],
    data: dict[tuple[str, str], list[str]],
    pmcid_pmid_dict: dict[str, str],
    pmid_pmcid_dict: dict[str, str],
) -> None:
    """Merge search results into the data dict."""
    for query, db, ids in results:
        for uid in ids:
            key = _resolve_uid(uid, db, pmcid_pmid_dict, pmid_pmcid_dict)
            data.setdefault(key, [])
            if query not in data[key]:
                data[key].append(query)


def search_queries(
    queries: list[str],
    data: dict[tuple[str, str], list[str]],
    previous_queries: set[str],
    pmcid_pmid_dict: dict[str, str],
    pmid_pmcid_dict: dict[str, str],
    email: str,
    min_date: str | None,
    save_every: int,
    save_filepath: str,
    api_key: str | None = None,
) -> dict[tuple[str, str], list[str]]:
    """Search PubMed/PMC for each query and collect article UIDs."""
    # Bio.Entrez stubs declare these module attrs as None-typed even though
    # the runtime expects strings; cast to Any to honor the runtime contract.
    entrez = cast("Any", Entrez)
    entrez.email = email
    if api_key:
        entrez.api_key = api_key
    _init_rate_limiter(api_key)

    new_queries = [q for q in queries if q not in previous_queries]
    log.info(
        "Searching %d new queries (%d skipped)...",
        len(new_queries),
        len(queries) - len(new_queries),
    )

    global _api_calls  # noqa: PLW0603
    _api_calls = 0
    t_start = time.monotonic()
    processed = 0
    pbar = tqdm(total=len(new_queries))
    _bar_done = threading.Event()

    def _refresh_bar() -> None:
        while not _bar_done.is_set():
            elapsed = time.monotonic() - t_start
            rate = _api_calls / elapsed if elapsed > 0 else 0
            pbar.set_postfix_str(f"API: {_api_calls} calls, {rate:.1f}/s")
            pbar.refresh()
            _bar_done.wait(timeout=1.0)

    bar_thread = threading.Thread(target=_refresh_bar, daemon=True)
    bar_thread.start()

    workers = 3 if api_key else 1
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_search_both_dbs, q, min_date): q for q in new_queries
            }
            for future in as_completed(futures):
                q = futures[future]
                try:
                    ok, results = future.result()
                    if not ok:
                        tqdm.write(
                            f"[WARNING] Partial failure for {q!r}"
                            " — will retry on next run"
                        )
                        continue
                    _merge_results(
                        results,
                        data,
                        pmcid_pmid_dict,
                        pmid_pmcid_dict,
                    )
                except Exception:
                    log.exception("Exception collecting results for %s", q)

                processed += 1
                pbar.update(1)
                if processed % save_every == 0:
                    save_data(data, save_filepath)
    finally:
        _bar_done.set()
        bar_thread.join()
        pbar.close()

    save_data(data, save_filepath)
    return data
