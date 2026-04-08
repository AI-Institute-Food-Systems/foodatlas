"""Sentence retrieval: extract food-chemical sentences from BioC-PMC articles."""

from __future__ import annotations

import json
import logging
from ast import literal_eval
from functools import partial
from itertools import islice
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any

import pandas as pd
from nltk.tokenize.punkt import PunktSentenceTokenizer
from thefuzz import fuzz
from tqdm import tqdm

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 5000

ALLOWED_SECTIONS = [
    "INTRO",
    "METHODS",
    "CONCL",
    "TITLE",
    "ABSTRACT",
    "RESULTS",
    "DISCUSS",
]

ALLOWED_TYPES = [
    "abstract",
    "paragraph",
]


def get_all_foods(filepath: str) -> dict[str, list[str]]:
    """Load food-to-translations mapping from a TSV file."""
    path = Path(filepath)
    if not path.is_file():
        msg = f"File not found: {filepath}"
        raise FileNotFoundError(msg)

    df = pd.read_csv(filepath, sep="\t", keep_default_na=False)
    df["translation"] = df["translation"].apply(literal_eval)
    foods_trans_dict: dict[str, list[str]] = dict(
        zip(df["query"], df["translation"], strict=True)
    )
    log.info("Got %d foods.", len(foods_trans_dict))
    return foods_trans_dict


def pmcid_to_filepath(pmcid: str, parent_dir: str) -> Path:
    """Convert a PMCID to the expected BioC file path."""
    return Path(parent_dir) / f"{pmcid}.xml"


def _is_valid_passage(passage: dict[str, Any]) -> bool:
    """Check if a BioC passage has valid section and type for filtering."""
    infons = passage.get("infons")
    if infons is None:
        return False
    section_type = infons.get("section_type")
    passage_type = infons.get("type")
    if section_type is None or passage_type is None:
        return False
    return section_type in ALLOWED_SECTIONS and passage_type in ALLOWED_TYPES


def _build_translated_queries(
    queries: list[str],
    foods_trans_dict: dict[str, list[str]],
) -> list[str]:
    """Build translated query list from food names dictionary."""
    translated: list[str] = []
    for q in queries:
        if q in foods_trans_dict:
            translated.extend(foods_trans_dict[q])
        else:
            translated.append(q)
    return translated


def get_filtered_sentences(
    sentence_tokenizer: PunktSentenceTokenizer,
    filepath_bioc_pmc: str,
    foods_trans_dict: dict[str, list[str]],
    key_val_pair: tuple[tuple[str, str], list[str]],
) -> pd.DataFrame:
    """Extract matching sentences from a single BioC-PMC article."""
    key, queries = key_val_pair
    _pmid, pmcid = key
    empty: dict[str, list[Any]] = {
        "pmcid": [],
        "section": [],
        "matched_query": [],
        "sentence": [],
    }

    if not pmcid.replace("PMC", "").isdigit():
        return pd.DataFrame(empty)

    filepath = pmcid_to_filepath(pmcid, filepath_bioc_pmc)
    if not filepath.is_file():
        return pd.DataFrame(empty)

    with filepath.open() as f:
        json_data = json.load(f)

    documents = json_data["documents"]
    if len(documents) != 1:
        msg = f"Expected 1 document, got {len(documents)}"
        raise ValueError(msg)

    document = documents[0]
    pmcid = document["id"]
    translated = _build_translated_queries(queries, foods_trans_dict)
    result: dict[str, list[Any]] = {
        "pmcid": [],
        "section": [],
        "matched_query": [],
        "sentence": [],
    }

    for passage in document["passages"]:
        if not _is_valid_passage(passage):
            continue
        section = passage["infons"]["section_type"]
        for sentence in sentence_tokenizer.tokenize(passage["text"]):
            if len(sentence) < 20 or len(sentence) > 1000:
                continue
            for tq in translated:
                if fuzz.token_set_ratio(sentence, tq) > 90:
                    result["pmcid"].append(pmcid)
                    result["section"].append(section)
                    result["matched_query"].append(tq)
                    result["sentence"].append(sentence)
                    break

    return pd.DataFrame(result)


def retrieve_sentences(
    query_uid_filepath: str,
    filepath_bioc_pmc: str,
    filepath_food_names: str,
    filtered_sentences_filepath: str,
) -> None:
    """Retrieve and filter sentences from BioC-PMC articles."""
    log.info("Using the query results to find the actual sentences...")

    foods_trans_dict = get_all_foods(filepath_food_names)

    log.info("Loading query results...")
    df = pd.read_csv(
        query_uid_filepath,
        sep="\t",
        dtype=str,
        keep_default_na=False,
    )
    df["key"] = list(zip(df["pmid"], df["pmcid"], strict=True))
    df["queries"] = df["queries"].apply(literal_eval)
    data: dict[tuple[str, str], list[str]] = dict(
        zip(df["key"], df["queries"], strict=True)
    )

    sentence_tokenizer = PunktSentenceTokenizer()

    def chunks(
        d: dict[tuple[str, str], list[str]],
        size: int = DEFAULT_CHUNK_SIZE,
    ) -> Any:
        it = iter(d)
        for _i in range(0, len(d), size):
            yield {k: d[k] for k in islice(it, size)}

    total_chunks = (len(data) + DEFAULT_CHUNK_SIZE - 1) // DEFAULT_CHUNK_SIZE
    for idx, small_data in enumerate(tqdm(chunks(data), total=total_chunks)):
        workers = max(1, cpu_count() - 1)
        with Pool(workers) as p:
            partial_function = partial(
                get_filtered_sentences,
                sentence_tokenizer,
                filepath_bioc_pmc,
                foods_trans_dict,
            )
            r = list(p.imap_unordered(partial_function, small_data.items()))

        chunk_df = pd.concat(r)
        chunk_df.to_csv(
            filtered_sentences_filepath.format(i=idx),
            sep="\t",
            index=False,
        )

    chunk_dir = Path(filtered_sentences_filepath).parent
    chunk_files = sorted(chunk_dir.glob("result_*.tsv"))
    if chunk_files:
        merged = pd.concat(
            [
                pd.read_csv(f, sep="\t", dtype=str, keep_default_na=False)
                for f in chunk_files
            ],
            ignore_index=True,
        )
    else:
        merged = pd.DataFrame(
            columns=["pmcid", "section", "matched_query", "sentence"],
        )
    out_path = chunk_dir / "sentence_filtering_input.tsv"
    merged.to_csv(out_path, sep="\t", index=False)
    log.info(
        "Merged %d chunk files -> %d rows -> %s",
        len(chunk_files),
        len(merged),
        out_path,
    )
