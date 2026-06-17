"""Sentence retrieval stage: extract food-chemical sentences from cached BioC files."""

from __future__ import annotations

from .sentence_retrieval import retrieve_sentences


def run_retrieval(
    *,
    query_uid_results_filepath: str,
    filtered_sentences_filepath: str,
    filepath_bioc_pmc: str,
    filepath_food_names: str,
) -> None:
    retrieve_sentences(
        query_uid_filepath=query_uid_results_filepath,
        filepath_bioc_pmc=filepath_bioc_pmc,
        filepath_food_names=filepath_food_names,
        filtered_sentences_filepath=filtered_sentences_filepath,
    )
