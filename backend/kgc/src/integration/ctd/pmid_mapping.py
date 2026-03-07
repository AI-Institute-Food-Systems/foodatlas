"""Step 2: PMID → PMCID mapping via NCBI ID converter."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pathlib import Path
import requests
from bs4 import BeautifulSoup

from .constants import PMCID, PMID_PMCID_REQUEST_URL, PUBMED_IDS

logger = logging.getLogger(__name__)

PMID_TO_PMCID_FILENAME = "CTD_pubmed_ids_to_pmcid.csv"
BATCH_SIZE = 200


def fetch_pmid_to_pmcid(
    pubmed_ids: list[int],
    email: str,
    tool_name: str = "foodatlas",
) -> pd.DataFrame:
    """Fetch PMID→PMCID mappings from NCBI in batches.

    Args:
        pubmed_ids: List of integer PubMed IDs.
        email: Contact email for the NCBI API.
        tool_name: Tool identifier for the NCBI API.

    Returns:
        DataFrame with ``pmid`` and ``pmcid`` columns.
    """
    middle_url = f"?tool={tool_name}&email={email}&ids="
    dfs: list[pd.DataFrame] = []

    for i in range(0, len(pubmed_ids), BATCH_SIZE):
        batch = pubmed_ids[i : i + BATCH_SIZE]
        ids_str = ",".join(str(pid) for pid in batch)
        url = PMID_PMCID_REQUEST_URL + middle_url + ids_str
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.error("NCBI request failed with status %d", response.status_code)
            continue

        soup = BeautifulSoup(response.content, "html.parser")
        records = soup.find_all("record")
        pmid_list = [record.get("pmid") for record in records]
        pmcid_list = [record.get("pmcid") for record in records]
        dfs.append(pd.DataFrame({PUBMED_IDS: pmid_list, PMCID: pmcid_list}))

    if not dfs:
        return pd.DataFrame(columns=[PUBMED_IDS, PMCID])

    result = pd.concat(dfs, ignore_index=True)
    return result.sort_values(by=PUBMED_IDS).reset_index(drop=True)


def load_pmid_to_pmcid(mapping_path: Path) -> dict[int, int]:
    """Load a PMID→PMCID CSV mapping, returning only valid entries.

    Args:
        mapping_path: Path to CSV with ``pmid`` and ``pmcid`` columns.

    Returns:
        Dict mapping integer PMID to integer PMCID.
    """
    df = pd.read_csv(mapping_path)
    df = df.dropna(how="all").reset_index(drop=True)
    df[PMCID] = df[PMCID].apply(
        lambda x: x.split("PMC")[1] if pd.notnull(x) and "PMC" in str(x) else None
    )
    df = df[df[PMCID].notnull()].reset_index(drop=True)
    df[PMCID] = df[PMCID].astype(int)
    df[PUBMED_IDS] = df[PUBMED_IDS].astype(int)
    return dict(zip(df[PUBMED_IDS], df[PMCID], strict=False))


def get_or_create_pmid_mapping(
    integration_dir: Path,
    pubmed_ids: list[int],
    email: str,
) -> dict[int, int]:
    """Load cached PMID→PMCID mapping or create it from NCBI.

    Args:
        integration_dir: Directory to cache the mapping CSV.
        pubmed_ids: PubMed IDs to map (used only when creating).
        email: NCBI contact email.

    Returns:
        Dict mapping integer PMID to integer PMCID.
    """
    cache_path = integration_dir / PMID_TO_PMCID_FILENAME
    if cache_path.exists():
        logger.info("Loading cached PMID→PMCID mapping from %s", cache_path)
        return load_pmid_to_pmcid(cache_path)

    logger.info("Fetching PMID→PMCID mapping from NCBI for %d IDs...", len(pubmed_ids))
    df = fetch_pmid_to_pmcid(pubmed_ids, email)
    integration_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False)
    return load_pmid_to_pmcid(cache_path)
