"""Search PubMed/PMC for food-chemical articles and retrieve sentences.

Thin orchestrator that delegates to pubmed_search and sentence_retrieval.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from .pubmed_search import (
    get_pmcid_pmid_mapping,
    load_data,
    parse_query,
    search_queries,
)
from .sentence_retrieval import retrieve_sentences

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_DATE_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}$")


def _latest_run_date(output_base_dir: str, current_date: str) -> str | None:
    """Find the most recent completed YYYY_MM_DD folder, excluding *current_date*.

    A folder is considered completed if it contains a
    ``sentence_filtering_input.tsv`` (output of the search stage).
    """
    base = Path(output_base_dir)
    if not base.is_dir():
        return None
    dates = sorted(
        d.name
        for d in base.iterdir()
        if d.is_dir()
        and _DATE_PATTERN.match(d.name)
        and d.name != current_date
        and (d / "retrieved_sentences" / "sentence_filtering_input.tsv").exists()
    )
    if not dates:
        return None
    return dates[-1].replace("_", "/")


def run_search(
    *,
    query: str,
    query_uid_results_filepath: str,
    filtered_sentences_filepath: str,
    filepath_bioc_pmc: str,
    filepath_food_names: str,
    output_base_dir: str,
    current_date: str,
    save_every: int = 50,
    min_date: str | None = None,
) -> None:
    """Run the PubMed/PMC search and sentence retrieval pipeline."""
    if min_date is None:
        min_date = _latest_run_date(output_base_dir, current_date)
        if min_date:
            log.info("Derived min_date from latest run folder: %s", min_date)

    pmcid_pmid_dict, pmid_pmcid_dict = get_pmcid_pmid_mapping()

    Path(query_uid_results_filepath).parent.mkdir(exist_ok=True, parents=True)
    Path(filtered_sentences_filepath).parent.mkdir(exist_ok=True, parents=True)

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

    retrieve_sentences(
        query_uid_filepath=query_uid_results_filepath,
        filepath_bioc_pmc=filepath_bioc_pmc,
        filepath_food_names=filepath_food_names,
        filtered_sentences_filepath=filtered_sentences_filepath,
    )
