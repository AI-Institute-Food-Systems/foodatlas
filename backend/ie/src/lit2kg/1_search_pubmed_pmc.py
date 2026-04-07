"""Step 1: Search PubMed/PMC for food-chemical articles and retrieve sentences.

Thin orchestrator that delegates to pubmed_search and sentence_retrieval modules.
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date
from pathlib import Path

from src.lit2kg.pubmed_search import (
    get_pmcid_pmid_mapping,
    load_data,
    parse_query,
    search_queries,
)
from src.lit2kg.sentence_retrieval import retrieve_sentences

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_SAVE_EVERY = 50
DEFAULT_EMAIL = os.environ.get("NCBI_EMAIL", "user@example.com")
DEFAULT_BIOC_PMC = os.environ.get("BIOC_PMC_DIR", "data/BioC-PMC")
DEFAULT_FOOD_NAMES = os.environ.get("FOOD_NAMES_FILE", "data/translated_food_terms.txt")
DEFAULT_LAST_SEARCH_DATE = "outputs/text_parser/last_search_date.txt"
DEFAULT_FILTERED_SENTENCES = "outputs/text_parser/retrieved_sentences/result_{i}.tsv"


def parse_argument() -> argparse.Namespace:
    """Parse CLI arguments for the PubMed/PMC search pipeline."""
    parser = argparse.ArgumentParser(
        description="Search PubMed/PMC and retrieve food-chemical sentences",
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Single query, comma-separated, or .txt file with one per line.",
    )
    parser.add_argument(
        "--save_every",
        type=int,
        default=DEFAULT_SAVE_EVERY,
        help=f"Save every N queries (default: {DEFAULT_SAVE_EVERY}).",
    )
    parser.add_argument(
        "--query_uid_results_filepath",
        type=str,
        default="outputs/text_parser/query_uid_results.tsv",
    )
    parser.add_argument("--email", type=str, default=DEFAULT_EMAIL)
    parser.add_argument(
        "--filepath_BioC_PMC",
        type=str,
        default=DEFAULT_BIOC_PMC,
    )
    parser.add_argument(
        "--filepath_food_names",
        type=str,
        default=DEFAULT_FOOD_NAMES,
    )
    parser.add_argument(
        "--filtered_sentences_filepath",
        type=str,
        default=DEFAULT_FILTERED_SENTENCES,
    )
    parser.add_argument("--min_date", type=str, default=None)
    parser.add_argument(
        "--last_search_date_filepath",
        type=str,
        default=DEFAULT_LAST_SEARCH_DATE,
    )
    return parser.parse_args()


def main() -> None:
    """Run the PubMed/PMC search and sentence retrieval pipeline."""
    args = parse_argument()

    if args.min_date is None and Path(args.last_search_date_filepath).is_file():
        args.min_date = Path(args.last_search_date_filepath).read_text().strip()
        log.info("Using saved min_date: %s", args.min_date)

    today = date.today().strftime("%Y/%m/%d")
    Path(args.last_search_date_filepath).parent.mkdir(
        exist_ok=True,
        parents=True,
    )
    Path(args.last_search_date_filepath).write_text(today)
    log.info("Recorded search date: %s -> %s", today, args.last_search_date_filepath)

    pmcid_pmid_dict, pmid_pmcid_dict = get_pmcid_pmid_mapping()

    Path(args.query_uid_results_filepath).parent.mkdir(
        exist_ok=True,
        parents=True,
    )
    Path(args.filtered_sentences_filepath).parent.mkdir(
        exist_ok=True,
        parents=True,
    )
    data, previous_queries = load_data(
        args.query_uid_results_filepath,
        pmcid_pmid_dict=pmcid_pmid_dict,
        pmid_pmcid_dict=pmid_pmcid_dict,
    )

    queries = parse_query(args.query)

    search_queries(
        queries=queries,
        data=data,
        previous_queries=previous_queries,
        pmcid_pmid_dict=pmcid_pmid_dict,
        pmid_pmcid_dict=pmid_pmcid_dict,
        email=args.email,
        min_date=args.min_date,
        save_every=args.save_every,
        save_filepath=args.query_uid_results_filepath,
    )

    retrieve_sentences(
        query_uid_filepath=args.query_uid_results_filepath,
        filepath_bioc_pmc=args.filepath_BioC_PMC,
        filepath_food_names=args.filepath_food_names,
        filtered_sentences_filepath=args.filtered_sentences_filepath,
    )


if __name__ == "__main__":
    main()
