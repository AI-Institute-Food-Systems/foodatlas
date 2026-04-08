"""Search PubMed/PMC for food-chemical articles and retrieve sentences.

Thin orchestrator that delegates to pubmed_search and sentence_retrieval.
"""

from __future__ import annotations

import logging
from datetime import date
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


def run_search(
    *,
    query: str,
    query_uid_results_filepath: str,
    filtered_sentences_filepath: str,
    filepath_bioc_pmc: str,
    filepath_food_names: str,
    email: str = "user@example.com",
    save_every: int = 50,
    min_date: str | None = None,
    last_search_date_filepath: str | None = None,
) -> None:
    """Run the PubMed/PMC search and sentence retrieval pipeline."""
    if (
        min_date is None
        and last_search_date_filepath
        and Path(last_search_date_filepath).is_file()
    ):
        min_date = Path(last_search_date_filepath).read_text().strip()
        log.info("Using saved min_date: %s", min_date)

    if last_search_date_filepath:
        today = date.today().strftime("%Y/%m/%d")
        Path(last_search_date_filepath).parent.mkdir(exist_ok=True, parents=True)
        Path(last_search_date_filepath).write_text(today)
        log.info("Recorded search date: %s -> %s", today, last_search_date_filepath)

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
        email=email,
        min_date=min_date,
        save_every=save_every,
        save_filepath=query_uid_results_filepath,
    )

    retrieve_sentences(
        query_uid_filepath=query_uid_results_filepath,
        filepath_bioc_pmc=filepath_bioc_pmc,
        filepath_food_names=filepath_food_names,
        filtered_sentences_filepath=filtered_sentences_filepath,
    )
