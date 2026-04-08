"""PubMed/PMC search utilities: query parsing, ID mapping, and E-utility calls."""

from __future__ import annotations

import logging
import subprocess
import time
from ast import literal_eval
from pathlib import Path
from typing import Any

import pandas as pd
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
    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
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
    for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
        pmid = row["pmid"]
        pmcid = row["pmcid"]

        if pmid and pmcid:
            if pmid_pmcid_dict[row["pmid"]] != pmcid:
                msg = f"PMCID mismatch for PMID {pmid}"
                raise ValueError(msg)
            if pmcid_pmid_dict[row["pmcid"]] != pmid:
                msg = f"PMID mismatch for PMCID {pmcid}"
                raise ValueError(msg)
        elif pmid == "":
            if pmcid in pmcid_pmid_dict:
                df.at[idx, "pmid"] = pmcid_pmid_dict[pmcid]
        elif pmcid == "":
            if pmid in pmid_pmcid_dict:
                df.at[idx, "pmcid"] = pmid_pmcid_dict[pmid]
        else:
            msg = f"Unexpected state: pmid={pmid!r}, pmcid={pmcid!r}"
            raise RuntimeError(msg)

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


def _search_single_db(
    db: str,
    query: str,
    email: str,
    min_date: str | None,
) -> list[str]:
    """Run esearch+efetch for one database, return list of UIDs."""
    esearch_args = [
        "esearch",
        "-db",
        db,
        "-query",
        QUERY_TEMPLATE.format(food=query),
        "-email",
        email,
        "-datetype",
        "pdat",
    ]
    if min_date:
        esearch_args += ["-mindate", min_date]
    esearch = subprocess.Popen(esearch_args, stdout=subprocess.PIPE)
    efetch_args = ["efetch", "-format", "uid", "-email", email]
    result = subprocess.check_output(
        efetch_args,
        stdin=esearch.stdout,
        text=True,
        timeout=120,
    )
    esearch.wait()
    return [f"PMC{uid}" if db == "pmc" else uid for uid in result.rstrip().split("\n")]


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
) -> dict[tuple[str, str], list[str]]:
    """Search PubMed/PMC for each query and collect article UIDs."""
    log.info("Searching queries and saving the UIDs...")
    pbar = tqdm(queries)
    for idx, q in enumerate(pbar):
        pbar.set_description(f"Processing {q}")
        if q in previous_queries:
            continue

        for db in ["pubmed", "pmc"]:
            try:
                ids = _search_single_db(db, q, email, min_date)
            except Exception:
                log.exception("Exception while processing %s", q)
                continue

            for uid in ids:
                key = _resolve_uid(
                    uid,
                    db,
                    pmcid_pmid_dict,
                    pmid_pmcid_dict,
                )
                data.setdefault(key, [])
                if q not in data[key]:
                    data[key].append(q)

        if idx % save_every == 0:
            save_data(data, save_filepath)

        time.sleep(0.33)

    save_data(data, save_filepath)
    return data
